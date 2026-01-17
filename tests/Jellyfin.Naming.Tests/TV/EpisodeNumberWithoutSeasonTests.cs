using Emby.Naming.Common;
using Emby.Naming.TV;
using Xunit;

namespace Jellyfin.Naming.Tests.TV
{
    public class EpisodeNumberWithoutSeasonTests
    {
        private readonly EpisodeResolver _resolver = new EpisodeResolver(new NamingOptions());

        [Theory]
        [InlineData(8, "The Simpsons/The Simpsons.S25E08.Steal this episode.mp4")]
        [InlineData(2, "The Simpsons/The Simpsons - 02 - Ep Name.avi")]
        [InlineData(2, "The Simpsons/02.avi")]
        [InlineData(2, "The Simpsons/02 - Ep Name.avi")]
        [InlineData(2, "The Simpsons/02-Ep Name.avi")]
        [InlineData(2, "The Simpsons/02.EpName.avi")]
        [InlineData(2, "The Simpsons/The Simpsons - 02.avi")]
        [InlineData(2, "The Simpsons/The Simpsons - 02 Ep Name.avi")]
        [InlineData(7, "GJ Club (2013)/GJ Club - 07.mkv")]
        [InlineData(17, "Case Closed (1996-2007)/Case Closed - 317.mkv")]
        // 4-digit episode numbers for long-running anime
        [InlineData(100, "Detective Conan/detective conan - 0100 [bluray].mkv")]
        [InlineData(1000, "Detective Conan/detective conan - 1000 [bluray].mkv")]
        [InlineData(1234, "One Piece/One Piece - 1234.mkv")]
        // TODO: [InlineData(2, @"The Simpsons/The Simpsons 5 - 02 - Ep Name.avi")]
        // TODO: [InlineData(2, @"The Simpsons/The Simpsons 5 - 02 Ep Name.avi")]
        // TODO: [InlineData(7, @"Seinfeld/Seinfeld 0807 The Checks.avi")]
        // This is not supported anymore after removing the episode number 365+ hack from EpisodePathParser
        // TODO: [InlineData(13, @"Case Closed (1996-2007)/Case Closed - 13.mkv")]
        public void GetEpisodeNumberFromFileTest(int episodeNumber, string path)
        {
            var result = _resolver.Resolve(path, false);

            Assert.Equal(episodeNumber, result?.EpisodeNumber);
        }
    }
}
