using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Data.Enums;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using Microsoft.Extensions.Logging;

namespace Emby.Server.Implementations.Library.Validators
{
    /// <summary>
    /// Automatically merges movies with the same metadata (IMDB/TMDB ID) into a single entry with multiple versions.
    /// </summary>
    public class MergeVersionsPostScanTask : ILibraryPostScanTask
    {
        private readonly ILibraryManager _libraryManager;
        private readonly ILogger<MergeVersionsPostScanTask> _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="MergeVersionsPostScanTask"/> class.
        /// </summary>
        /// <param name="libraryManager">The library manager.</param>
        /// <param name="logger">The logger.</param>
        public MergeVersionsPostScanTask(
            ILibraryManager libraryManager,
            ILogger<MergeVersionsPostScanTask> logger)
        {
            _libraryManager = libraryManager;
            _logger = logger;
        }

        /// <inheritdoc />
        public async Task Run(IProgress<double> progress, CancellationToken cancellationToken)
        {
            // Dictionary to track movies by their IMDB ID across all libraries
            var imdbMoviesMap = new Dictionary<string, List<Video>>(StringComparer.OrdinalIgnoreCase);
            var tmdbMoviesMap = new Dictionary<string, List<Video>>(StringComparer.OrdinalIgnoreCase);

            var enabledLibraries = new HashSet<Guid>();

            // First pass: collect all movies from libraries with the option enabled
            foreach (var library in _libraryManager.RootFolder.Children)
            {
                var libraryOptions = _libraryManager.GetLibraryOptions(library);
                if (libraryOptions?.MergeMoviesWithSameMetadata != true)
                {
                    continue;
                }

                enabledLibraries.Add(library.Id);

                var startIndex = 0;
                const int PageSize = 1000;

                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var movies = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        MediaTypes = new[] { MediaType.Video },
                        IncludeItemTypes = new[] { BaseItemKind.Movie },
                        IsVirtualItem = false,
                        HasImdbId = true,
                        Parent = library,
                        StartIndex = startIndex,
                        Limit = PageSize,
                        Recursive = true
                    });

                    foreach (var item in movies)
                    {
                        if (item is not Video video)
                        {
                            continue;
                        }

                        // Skip items that are already alternate versions of another item
                        if (!string.IsNullOrEmpty(video.PrimaryVersionId))
                        {
                            continue;
                        }

                        var imdbId = video.GetProviderId(MetadataProvider.Imdb);
                        if (!string.IsNullOrEmpty(imdbId))
                        {
                            if (!imdbMoviesMap.TryGetValue(imdbId, out var list))
                            {
                                list = new List<Video>();
                                imdbMoviesMap[imdbId] = list;
                            }

                            list.Add(video);
                        }
                        else
                        {
                            // Fall back to TMDB ID if no IMDB ID
                            var tmdbId = video.GetProviderId(MetadataProvider.Tmdb);
                            if (!string.IsNullOrEmpty(tmdbId))
                            {
                                if (!tmdbMoviesMap.TryGetValue(tmdbId, out var list))
                                {
                                    list = new List<Video>();
                                    tmdbMoviesMap[tmdbId] = list;
                                }

                                list.Add(video);
                            }
                        }
                    }

                    if (movies.Count < PageSize)
                    {
                        break;
                    }

                    startIndex += PageSize;
                }
            }

            // Combine TMDB movies into IMDB map for processing (IMDB takes precedence)
            foreach (var (tmdbId, movies) in tmdbMoviesMap)
            {
                // Only process if there are duplicates and none of them have IMDB IDs
                if (movies.Count > 1 && movies.All(m => string.IsNullOrEmpty(m.GetProviderId(MetadataProvider.Imdb))))
                {
                    // Use TMDB ID as key with prefix to avoid conflicts
                    imdbMoviesMap[$"tmdb_{tmdbId}"] = movies;
                }
            }

            // Filter to only groups with duplicates
            var duplicateGroups = imdbMoviesMap.Where(kvp => kvp.Value.Count > 1).ToList();

            if (duplicateGroups.Count == 0)
            {
                _logger.LogInformation("No duplicate movies found to merge");
                progress.Report(100);
                return;
            }

            _logger.LogInformation("Found {Count} groups of duplicate movies to merge", duplicateGroups.Count);

            var numComplete = 0;
            var count = duplicateGroups.Count;

            foreach (var (providerId, movies) in duplicateGroups)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    await MergeMoviesAsync(movies, cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation("Merged {Count} versions of movie with provider ID {ProviderId}", movies.Count, providerId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error merging movies with provider ID {ProviderId}", providerId);
                }

                numComplete++;
                progress.Report((double)numComplete / count * 100);
            }

            progress.Report(100);
        }

        private async Task MergeMoviesAsync(List<Video> movies, CancellationToken cancellationToken)
        {
            if (movies.Count < 2)
            {
                return;
            }

            // Sort movies to determine primary version
            // Prefer: already has multiple sources > highest resolution > standard video type
            var sortedMovies = movies
                .OrderByDescending(m => m.MediaSourceCount > 1 && string.IsNullOrEmpty(m.PrimaryVersionId) ? 1 : 0)
                .ThenBy(m => m.Video3DFormat.HasValue || m.VideoType != VideoType.VideoFile ? 1 : 0)
                .ThenByDescending(m => m.GetDefaultVideoStream()?.Width ?? 0)
                .ToList();

            var primaryVersion = sortedMovies.First();
            var alternateVersionsOfPrimary = primaryVersion.LinkedAlternateVersions.ToList();

            foreach (var item in sortedMovies.Skip(1))
            {
                // Set the primary version ID on the alternate
                item.SetPrimaryVersionId(primaryVersion.Id.ToString("N", CultureInfo.InvariantCulture));

                await item.UpdateToRepositoryAsync(ItemUpdateType.MetadataEdit, cancellationToken).ConfigureAwait(false);

                // Add to primary's linked alternate versions if not already there
                if (!alternateVersionsOfPrimary.Any(i => string.Equals(i.Path, item.Path, StringComparison.OrdinalIgnoreCase)))
                {
                    alternateVersionsOfPrimary.Add(new LinkedChild
                    {
                        Path = item.Path,
                        ItemId = item.Id
                    });
                }

                // Also add any existing linked alternates from this item
                foreach (var linkedItem in item.LinkedAlternateVersions)
                {
                    if (!alternateVersionsOfPrimary.Any(i => string.Equals(i.Path, linkedItem.Path, StringComparison.OrdinalIgnoreCase)))
                    {
                        alternateVersionsOfPrimary.Add(linkedItem);
                    }
                }

                // Clear the item's own linked alternate versions since it's now an alternate itself
                if (item.LinkedAlternateVersions.Length > 0)
                {
                    item.LinkedAlternateVersions = Array.Empty<LinkedChild>();
                    await item.UpdateToRepositoryAsync(ItemUpdateType.MetadataEdit, cancellationToken).ConfigureAwait(false);
                }
            }

            // Update the primary version with all linked alternates
            primaryVersion.LinkedAlternateVersions = alternateVersionsOfPrimary.ToArray();
            await primaryVersion.UpdateToRepositoryAsync(ItemUpdateType.MetadataEdit, cancellationToken).ConfigureAwait(false);
        }
    }
}
