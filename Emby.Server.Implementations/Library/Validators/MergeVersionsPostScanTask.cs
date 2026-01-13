using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Data.Enums;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using Microsoft.Extensions.Logging;

namespace Emby.Server.Implementations.Library.Validators
{
    /// <summary>
    /// Automatically merges movies and episodes with the same metadata (any matching provider ID) into a single entry with multiple versions.
    /// </summary>
    public class MergeVersionsPostScanTask : ILibraryPostScanTask
    {
        private readonly ILibraryManager _libraryManager;
        private readonly ILogger<MergeVersionsPostScanTask> _logger;

        // Provider IDs that identify the actual content (not collections/boxsets)
        // Only these should be used for duplicate detection
        private static readonly HashSet<string> _validProviderNames = new(StringComparer.OrdinalIgnoreCase)
        {
            "Imdb",
            "Tmdb",
            "Tvdb",
            "Zap2It",
            "TvRage",
            "TvMaze",
            "Trakt"
        };

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
            _logger.LogInformation("MergeVersionsPostScanTask: Starting to scan for duplicate videos to merge");

            // Collect all videos from all libraries with the option enabled
            var allMovies = new List<Video>();
            var allEpisodes = new List<Video>();

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

                // Collect movies
                CollectVideosFromLibrary(library, BaseItemKind.Movie, allMovies, cancellationToken);

                // Collect episodes
                CollectVideosFromLibrary(library, BaseItemKind.Episode, allEpisodes, cancellationToken);
            }

            _logger.LogInformation("MergeVersionsPostScanTask: Total movies collected: {Count}", allMovies.Count);
            _logger.LogInformation("MergeVersionsPostScanTask: Total episodes collected: {Count}", allEpisodes.Count);

            // Process movies
            var movieGroups = ProcessVideoGroup(allMovies, "movies");

            // Process episodes - group by series and season first, then by provider ID within each group
            var episodeGroups = ProcessEpisodes(allEpisodes);

            var allGroups = movieGroups.Concat(episodeGroups).ToList();

            if (allGroups.Count == 0)
            {
                _logger.LogInformation("MergeVersionsPostScanTask: No duplicate videos found to merge");
                progress.Report(100);
                return;
            }

            _logger.LogInformation("MergeVersionsPostScanTask: Found {Count} total groups of duplicate videos to merge", allGroups.Count);

            var numComplete = 0;
            var count = allGroups.Count;

            foreach (var group in allGroups)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var videoNames = string.Join(", ", group.Select(m => $"'{m.Name}' ({m.Path})"));
                _logger.LogInformation("MergeVersionsPostScanTask: Merging group of {Count} videos: {Videos}", group.Count, videoNames);

                try
                {
                    await MergeVideosAsync(group, cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation("MergeVersionsPostScanTask: Successfully merged {Count} versions", group.Count);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "MergeVersionsPostScanTask: Error merging videos: {Videos}", videoNames);
                }

                numComplete++;
                progress.Report((double)numComplete / count * 100);
            }

            _logger.LogInformation("MergeVersionsPostScanTask: Completed. Merged {Count} groups of duplicate videos", allGroups.Count);
            progress.Report(100);
        }

        private void CollectVideosFromLibrary(BaseItem library, BaseItemKind itemKind, List<Video> videoList, CancellationToken cancellationToken)
        {
            var startIndex = 0;
            const int PageSize = 1000;
            var itemTypeName = itemKind.ToString().ToLowerInvariant() + "s";

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var videos = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    MediaTypes = new[] { MediaType.Video },
                    IncludeItemTypes = new[] { itemKind },
                    IsVirtualItem = false,
                    Parent = library,
                    StartIndex = startIndex,
                    Limit = PageSize,
                    Recursive = true
                });

                _logger.LogInformation(
                    "MergeVersionsPostScanTask: Retrieved {Count} {ItemType} from library '{LibraryName}' (startIndex: {StartIndex})",
                    videos.Count,
                    itemTypeName,
                    library.Name,
                    startIndex);

                foreach (var item in videos)
                {
                    if (item is Video video)
                    {
                        // Skip items that are already alternate versions of another item
                        if (!string.IsNullOrEmpty(video.PrimaryVersionId))
                        {
                            _logger.LogDebug(
                                "MergeVersionsPostScanTask: Skipping '{VideoName}' - already an alternate version (PrimaryVersionId: {PrimaryVersionId})",
                                video.Name,
                                video.PrimaryVersionId);
                            continue;
                        }

                        videoList.Add(video);

                        // Log provider IDs for this video
                        var providerIds = video.ProviderIds;
                        if (providerIds != null && providerIds.Count > 0)
                        {
                            var providerIdStr = string.Join(", ", providerIds.Select(p => $"{p.Key}={p.Value}"));
                            _logger.LogDebug(
                                "MergeVersionsPostScanTask: Found {ItemType} '{VideoName}' with provider IDs: {ProviderIds}",
                                itemKind,
                                video.Name,
                                providerIdStr);
                        }
                        else
                        {
                            _logger.LogDebug(
                                "MergeVersionsPostScanTask: Found {ItemType} '{VideoName}' with NO provider IDs",
                                itemKind,
                                video.Name);
                        }
                    }
                }

                if (videos.Count < PageSize)
                {
                    break;
                }

                startIndex += PageSize;
            }
        }

        private List<List<Video>> ProcessVideoGroup(List<Video> videos, string typeName)
        {
            if (videos.Count < 2)
            {
                _logger.LogInformation("MergeVersionsPostScanTask: Not enough {TypeName} to merge (need at least 2)", typeName);
                return new List<List<Video>>();
            }

            // Group videos by matching provider IDs
            var duplicateGroups = FindDuplicateGroups(videos);

            if (duplicateGroups.Count == 0)
            {
                _logger.LogInformation("MergeVersionsPostScanTask: No duplicate {TypeName} found to merge", typeName);
            }
            else
            {
                _logger.LogInformation("MergeVersionsPostScanTask: Found {Count} groups of duplicate {TypeName} to merge", duplicateGroups.Count, typeName);
            }

            return duplicateGroups;
        }

        private List<List<Video>> ProcessEpisodes(List<Video> episodes)
        {
            if (episodes.Count < 2)
            {
                _logger.LogInformation("MergeVersionsPostScanTask: Not enough episodes to merge (need at least 2)");
                return new List<List<Video>>();
            }

            var allGroups = new List<List<Video>>();

            // Group episodes by series and season/episode number first
            // This ensures we only merge episodes that are actually the same episode
            var episodesByKey = new Dictionary<string, List<Video>>(StringComparer.OrdinalIgnoreCase);

            foreach (var video in episodes)
            {
                if (video is Episode episode)
                {
                    // Create a key based on series ID + season number + episode number
                    var seriesId = episode.SeriesId;
                    var seasonNumber = episode.ParentIndexNumber ?? -1;
                    var episodeNumber = episode.IndexNumber ?? -1;

                    // Also consider IndexNumberEnd for multi-episode files
                    var episodeNumberEnd = episode.IndexNumberEnd ?? episodeNumber;

                    var key = $"{seriesId}|S{seasonNumber}|E{episodeNumber}-{episodeNumberEnd}";

                    if (!episodesByKey.TryGetValue(key, out var episodeList))
                    {
                        episodeList = new List<Video>();
                        episodesByKey[key] = episodeList;
                    }

                    episodeList.Add(video);

                    _logger.LogDebug(
                        "MergeVersionsPostScanTask: Episode '{EpisodeName}' grouped under key '{Key}'",
                        episode.Name,
                        key);
                }
            }

            // Now process each group of same-episode videos
            foreach (var (key, episodeGroup) in episodesByKey)
            {
                if (episodeGroup.Count < 2)
                {
                    continue;
                }

                // For episodes with the same series/season/episode, also verify they have matching provider IDs
                // or are truly the same episode
                var duplicateGroups = FindDuplicateGroups(episodeGroup);

                // If no provider ID matches found, but episodes have same series/season/episode number,
                // they are likely the same episode with different file versions
                if (duplicateGroups.Count == 0 && episodeGroup.Count >= 2)
                {
                    // Check if any episodes in this group have NO provider IDs
                    // If so, group them all together since they share the same episode identifiers
                    var episodesWithoutProviderIds = episodeGroup
                        .Where(e => e.ProviderIds == null || e.ProviderIds.Count == 0 ||
                                    !e.ProviderIds.Any(p => _validProviderNames.Contains(p.Key) && !string.IsNullOrEmpty(p.Value)))
                        .ToList();

                    if (episodesWithoutProviderIds.Count > 0 || episodeGroup.All(e => e.ProviderIds == null || e.ProviderIds.Count == 0))
                    {
                        // All episodes in this group are the same based on series/season/episode
                        _logger.LogInformation(
                            "MergeVersionsPostScanTask: Grouping {Count} episodes with key '{Key}' by episode identifiers (no provider IDs)",
                            episodeGroup.Count,
                            key);
                        allGroups.Add(episodeGroup);
                    }
                }
                else
                {
                    allGroups.AddRange(duplicateGroups);
                }
            }

            if (allGroups.Count == 0)
            {
                _logger.LogInformation("MergeVersionsPostScanTask: No duplicate episodes found to merge");
            }
            else
            {
                _logger.LogInformation("MergeVersionsPostScanTask: Found {Count} groups of duplicate episodes to merge", allGroups.Count);
            }

            return allGroups;
        }

        /// <summary>
        /// Finds groups of videos that share at least one provider ID.
        /// </summary>
        private List<List<Video>> FindDuplicateGroups(List<Video> videos)
        {
            // Use Union-Find to group videos with matching provider IDs
            var parent = new Dictionary<int, int>();
            var rank = new Dictionary<int, int>();

            int Find(int x)
            {
                if (!parent.TryGetValue(x, out var p))
                {
                    parent[x] = x;
                    rank[x] = 0;
                    return x;
                }

                if (p != x)
                {
                    parent[x] = Find(p);
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

            // Build index of provider ID -> video indices
            var providerIdToVideos = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < videos.Count; i++)
            {
                var video = videos[i];
                var providerIds = video.ProviderIds;

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

                    // Skip collection/boxset provider IDs - they indicate videos in the same series, not duplicates
                    if (!_validProviderNames.Contains(provider))
                    {
                        _logger.LogDebug(
                            "MergeVersionsPostScanTask: Skipping provider '{Provider}' for video '{VideoName}' - not a content identifier",
                            provider,
                            video.Name);
                        continue;
                    }

                    // Create a unique key combining provider name and ID
                    var key = $"{provider}:{id}";

                    if (!providerIdToVideos.TryGetValue(key, out var videoIndices))
                    {
                        videoIndices = new List<int>();
                        providerIdToVideos[key] = videoIndices;
                    }

                    videoIndices.Add(i);
                }
            }

            // Union videos that share any provider ID
            foreach (var (providerKey, videoIndices) in providerIdToVideos)
            {
                if (videoIndices.Count < 2)
                {
                    continue;
                }

                _logger.LogDebug(
                    "MergeVersionsPostScanTask: Provider ID '{ProviderKey}' is shared by {Count} videos",
                    providerKey,
                    videoIndices.Count);

                var first = videoIndices[0];
                for (var i = 1; i < videoIndices.Count; i++)
                {
                    Union(first, videoIndices[i]);
                }
            }

            // Group videos by their root parent
            var groups = new Dictionary<int, List<Video>>();
            for (var i = 0; i < videos.Count; i++)
            {
                var video = videos[i];
                if (video.ProviderIds == null || video.ProviderIds.Count == 0)
                {
                    continue;
                }

                var root = Find(i);
                if (!groups.TryGetValue(root, out var group))
                {
                    group = new List<Video>();
                    groups[root] = group;
                }

                group.Add(video);
            }

            // Return only groups with more than one video (actual duplicates)
            return groups.Values.Where(g => g.Count > 1).ToList();
        }

        private async Task MergeVideosAsync(List<Video> videos, CancellationToken cancellationToken)
        {
            if (videos.Count < 2)
            {
                return;
            }

            // Sort videos to determine primary version
            // Prefer: already has multiple sources > highest resolution > standard video type
            var sortedVideos = videos
                .OrderByDescending(m => m.MediaSourceCount > 1 && string.IsNullOrEmpty(m.PrimaryVersionId) ? 1 : 0)
                .ThenBy(m => m.Video3DFormat.HasValue || m.VideoType != VideoType.VideoFile ? 1 : 0)
                .ThenByDescending(m => m.GetDefaultVideoStream()?.Width ?? 0)
                .ToList();

            var primaryVersion = sortedVideos.First();
            _logger.LogInformation(
                "MergeVersionsPostScanTask: Selected primary version: '{VideoName}' ({Path})",
                primaryVersion.Name,
                primaryVersion.Path);

            var alternateVersionsOfPrimary = primaryVersion.LinkedAlternateVersions.ToList();

            foreach (var item in sortedVideos.Skip(1))
            {
                _logger.LogInformation(
                    "MergeVersionsPostScanTask: Linking '{VideoName}' ({Path}) as alternate version",
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
                "MergeVersionsPostScanTask: Primary version '{VideoName}' now has {Count} alternate versions",
                primaryVersion.Name,
                alternateVersionsOfPrimary.Count);
        }
    }
}
