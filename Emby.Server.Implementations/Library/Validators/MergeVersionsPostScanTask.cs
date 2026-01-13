using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Data.Enums;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using Microsoft.Extensions.Logging;

namespace Emby.Server.Implementations.Library.Validators
{
    /// <summary>
    /// Automatically merges movies with the same metadata (any matching provider ID) into a single entry with multiple versions.
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
            _logger.LogInformation("MergeVersionsPostScanTask: Starting to scan for duplicate movies to merge");

            // Collect all movies from all libraries with the option enabled
            var allMovies = new List<Video>();

            foreach (var library in _libraryManager.RootFolder.Children)
            {
                var libraryOptions = _libraryManager.GetLibraryOptions(library);
                _logger.LogInformation(
                    "MergeVersionsPostScanTask: Library '{LibraryName}' (ID: {LibraryId}) - MergeMoviesWithSameMetadata = {MergeEnabled}",
                    library.Name,
                    library.Id,
                    libraryOptions?.MergeMoviesWithSameMetadata);

                if (libraryOptions?.MergeMoviesWithSameMetadata != true)
                {
                    _logger.LogInformation("MergeVersionsPostScanTask: Skipping library '{LibraryName}' - merge option not enabled", library.Name);
                    continue;
                }

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
                        Parent = library,
                        StartIndex = startIndex,
                        Limit = PageSize,
                        Recursive = true
                    });

                    _logger.LogInformation(
                        "MergeVersionsPostScanTask: Retrieved {Count} movies from library '{LibraryName}' (startIndex: {StartIndex})",
                        movies.Count,
                        library.Name,
                        startIndex);

                    foreach (var item in movies)
                    {
                        if (item is Video video)
                        {
                            // Skip items that are already alternate versions of another item
                            if (!string.IsNullOrEmpty(video.PrimaryVersionId))
                            {
                                _logger.LogDebug(
                                    "MergeVersionsPostScanTask: Skipping '{MovieName}' - already an alternate version (PrimaryVersionId: {PrimaryVersionId})",
                                    video.Name,
                                    video.PrimaryVersionId);
                                continue;
                            }

                            allMovies.Add(video);

                            // Log provider IDs for this movie
                            var providerIds = video.ProviderIds;
                            if (providerIds != null && providerIds.Count > 0)
                            {
                                var providerIdStr = string.Join(", ", providerIds.Select(p => $"{p.Key}={p.Value}"));
                                _logger.LogDebug(
                                    "MergeVersionsPostScanTask: Found movie '{MovieName}' with provider IDs: {ProviderIds}",
                                    video.Name,
                                    providerIdStr);
                            }
                            else
                            {
                                _logger.LogDebug(
                                    "MergeVersionsPostScanTask: Found movie '{MovieName}' with NO provider IDs",
                                    video.Name);
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

            _logger.LogInformation("MergeVersionsPostScanTask: Total movies collected: {Count}", allMovies.Count);

            if (allMovies.Count < 2)
            {
                _logger.LogInformation("MergeVersionsPostScanTask: Not enough movies to merge (need at least 2)");
                progress.Report(100);
                return;
            }

            // Group movies by matching provider IDs
            var duplicateGroups = FindDuplicateGroups(allMovies);

            if (duplicateGroups.Count == 0)
            {
                _logger.LogInformation("MergeVersionsPostScanTask: No duplicate movies found to merge");
                progress.Report(100);
                return;
            }

            _logger.LogInformation("MergeVersionsPostScanTask: Found {Count} groups of duplicate movies to merge", duplicateGroups.Count);

            var numComplete = 0;
            var count = duplicateGroups.Count;

            foreach (var group in duplicateGroups)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var movieNames = string.Join(", ", group.Select(m => $"'{m.Name}' ({m.Path})"));
                _logger.LogInformation("MergeVersionsPostScanTask: Merging group of {Count} movies: {Movies}", group.Count, movieNames);

                try
                {
                    await MergeMoviesAsync(group, cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation("MergeVersionsPostScanTask: Successfully merged {Count} versions", group.Count);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "MergeVersionsPostScanTask: Error merging movies: {Movies}", movieNames);
                }

                numComplete++;
                progress.Report((double)numComplete / count * 100);
            }

            _logger.LogInformation("MergeVersionsPostScanTask: Completed. Merged {Count} groups of duplicate movies", duplicateGroups.Count);
            progress.Report(100);
        }

        /// <summary>
        /// Finds groups of movies that share at least one provider ID.
        /// </summary>
        private List<List<Video>> FindDuplicateGroups(List<Video> movies)
        {
            // Use Union-Find to group movies with matching provider IDs
            var parent = new Dictionary<int, int>();
            var rank = new Dictionary<int, int>();

            int Find(int x)
            {
                if (!parent.ContainsKey(x))
                {
                    parent[x] = x;
                    rank[x] = 0;
                }

                if (parent[x] != x)
                {
                    parent[x] = Find(parent[x]);
                }

                return parent[x];
            }

            void Union(int x, int y)
            {
                var px = Find(x);
                var py = Find(y);
                if (px == py)
                {
                    return;
                }

                if (rank[px] < rank[py])
                {
                    parent[px] = py;
                }
                else if (rank[px] > rank[py])
                {
                    parent[py] = px;
                }
                else
                {
                    parent[py] = px;
                    rank[px]++;
                }
            }

            // Build index of provider ID -> movie indices
            var providerIdToMovies = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < movies.Count; i++)
            {
                var movie = movies[i];
                var providerIds = movie.ProviderIds;

                if (providerIds == null || providerIds.Count == 0)
                {
                    continue;
                }

                foreach (var (provider, id) in providerIds)
                {
                    if (string.IsNullOrEmpty(id))
                    {
                        continue;
                    }

                    // Create a unique key combining provider name and ID
                    var key = $"{provider}:{id}";

                    if (!providerIdToMovies.TryGetValue(key, out var movieIndices))
                    {
                        movieIndices = new List<int>();
                        providerIdToMovies[key] = movieIndices;
                    }

                    movieIndices.Add(i);
                }
            }

            // Union movies that share any provider ID
            foreach (var (providerKey, movieIndices) in providerIdToMovies)
            {
                if (movieIndices.Count < 2)
                {
                    continue;
                }

                _logger.LogDebug(
                    "MergeVersionsPostScanTask: Provider ID '{ProviderKey}' is shared by {Count} movies",
                    providerKey,
                    movieIndices.Count);

                var first = movieIndices[0];
                for (var i = 1; i < movieIndices.Count; i++)
                {
                    Union(first, movieIndices[i]);
                }
            }

            // Group movies by their root parent
            var groups = new Dictionary<int, List<Video>>();
            for (var i = 0; i < movies.Count; i++)
            {
                var movie = movies[i];
                if (movie.ProviderIds == null || movie.ProviderIds.Count == 0)
                {
                    continue;
                }

                var root = Find(i);
                if (!groups.TryGetValue(root, out var group))
                {
                    group = new List<Video>();
                    groups[root] = group;
                }

                group.Add(movie);
            }

            // Return only groups with more than one movie (actual duplicates)
            return groups.Values.Where(g => g.Count > 1).ToList();
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
            _logger.LogInformation(
                "MergeVersionsPostScanTask: Selected primary version: '{MovieName}' ({Path})",
                primaryVersion.Name,
                primaryVersion.Path);

            var alternateVersionsOfPrimary = primaryVersion.LinkedAlternateVersions.ToList();

            foreach (var item in sortedMovies.Skip(1))
            {
                _logger.LogInformation(
                    "MergeVersionsPostScanTask: Linking '{MovieName}' ({Path}) as alternate version",
                    item.Name,
                    item.Path);

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

            _logger.LogInformation(
                "MergeVersionsPostScanTask: Primary version '{MovieName}' now has {Count} alternate versions",
                primaryVersion.Name,
                alternateVersionsOfPrimary.Count);
        }
    }
}
