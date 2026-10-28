/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

@SuppressWarnings("RegexpMultiline")
public class StoragePathTests extends ESTestCase {

    // -- isPattern --

    /**
     * {@code literalNamePrefix} is the text a matching key must carry immediately after {@code patternPrefix}.
     * <p>
     * It is handed to the store to narrow a listing, so a value that is too <em>long</em> excludes files the
     * pattern matches — data loss, not a slowdown. Every case below pins it against that: the prefix must never
     * extend past the first metacharacter of the first patterned segment.
     */
    public void testLiteralNamePrefixStopsAtTheFirstMetacharacter() {
        assertEquals("date=2024-01-0", StoragePath.of("s3://b/data/date=2024-01-0*/*.parquet").literalNamePrefix());
        assertEquals("part-", StoragePath.of("s3://b/data/part-*.csv").literalNamePrefix());
        assertEquals("f", StoragePath.of("s3://b/data/f?.csv").literalNamePrefix());
        assertEquals("log", StoragePath.of("s3://b/data/log[0-9].csv").literalNamePrefix());
        assertEquals("a", StoragePath.of("s3://b/data/a{x,y}.csv").literalNamePrefix());
    }

    /** A segment that begins with a metacharacter narrows nothing, so the prefix is empty rather than wrong. */
    public void testLiteralNamePrefixIsEmptyWhenTheSegmentOpensWithAMetacharacter() {
        assertEquals("", StoragePath.of("s3://b/data/*.parquet").literalNamePrefix());
        assertEquals("", StoragePath.of("s3://b/data/*/part.csv").literalNamePrefix());
        assertEquals("", StoragePath.of("s3://b/data/?x.csv").literalNamePrefix());
        assertEquals("", StoragePath.of("s3://b/data/[ab]/f.csv").literalNamePrefix());
    }

    /** A globstar leads, so nothing after it is a property of the first segment. */
    public void testLiteralNamePrefixIsEmptyUnderALeadingGlobstar() {
        assertEquals("", StoragePath.of("s3://b/data/" + "**" + "/*.parquet").literalNamePrefix());
    }

    /**
     * A file URI built over a Windows-shaped absolute path parses, with and without a {@code localhost} authority.
     * <p>
     * Pinned platform-independently because the trap is not the parsing but the spelling: concatenating
     * {@code "file://localhost"} with {@code Path#toAbsolutePath} gives {@code file://localhostC:\...} on Windows,
     * whose authority this rejects, and a test that builds its URI that way fails only on a Windows run.
     * {@link StoragePath#fileUri} is what normalises the separators and the leading slash.
     */
    public void testAWindowsShapedFileUriParsesWithAndWithoutAnAuthority() {
        String windowsStyle = "/C:/Users/jenkins/tmp/dir";
        // Each spelling against the local path it must yield. Asserted on localPath() rather than toString(),
        // which returns the location verbatim and so contains the drive letter whatever the parse did with it.
        // Two separate mechanisms produce these, and the spellings are chosen so that both are exercised: where
        // the drive letter is already in the path it arrives with no leading slash because localPath() strips one
        // it cannot hand to the OS, and in the last spelling -- which is the one two slashes rather than three
        // produce, and what the countfs double's toFileScheme builds -- "C:" is read as the authority and only the
        // fold in StoragePath#of puts it back in the path. Without the fold that spelling does not resolve at all:
        // the authority splits at its last colon into host "C" and an empty port, and parsing that port throws.
        Map<String, String> expected = Map.of(
            "file://" + windowsStyle,
            "C:/Users/jenkins/tmp/dir",
            "file://" + windowsStyle + "/",
            "C:/Users/jenkins/tmp/dir/",
            "file://localhost" + windowsStyle,
            "C:/Users/jenkins/tmp/dir",
            "file://localhost" + windowsStyle + "/",
            "C:/Users/jenkins/tmp/dir/",
            "file://" + windowsStyle + "//",
            "C:/Users/jenkins/tmp/dir//",
            "file://C:/Users/jenkins/tmp/dir",
            "C:/Users/jenkins/tmp/dir"
        );
        for (Map.Entry<String, String> spelling : expected.entrySet()) {
            StoragePath path = StoragePath.of(spelling.getKey());
            assertEquals(spelling.getKey(), "file", path.scheme());
            assertEquals(spelling.getKey(), spelling.getValue(), path.localPath());
        }
        // And the spelling that skipping fileUri would produce is rejected, which is what makes the normalisation
        // load-bearing rather than tidy: the authority reads as "localhostC:".
        expectThrows(IllegalArgumentException.class, () -> StoragePath.of("file://localhostC:/Users/jenkins/tmp/dir"));
    }

    /**
     * The normalisation the test above exercises runs only for scheme {@code file}, so a location for any other
     * scheme has to be built from {@link StoragePath#fileUri} rather than by concatenating an absolute path onto
     * the scheme. Pinned here because nothing else can catch it cheaply: on POSIX the two spellings are the same
     * string, so only a Windows run tells them apart, and a test that needs Windows to fail is a test that reports
     * late.
     */
    public void testANonFileSchemeDoesNotFoldAWindowsDriveLetter() {
        String windowsAbsolute = "C:\\Users\\jenkins\\tmp\\dir";
        IllegalArgumentException rejected = expectThrows(
            IllegalArgumentException.class,
            () -> StoragePath.of("countfs://" + windowsAbsolute + "/date=*/*.csv")
        );
        assertThat(rejected.getMessage(), containsString("Malformed authority"));
        // Spelled the way fileUri spells it, the same directory parses under a scheme that is not file.
        StoragePath path = StoragePath.of("countfs:///C:/Users/jenkins/tmp/dir/date=*/*.csv");
        assertEquals("countfs", path.scheme());
        assertEquals("C:/Users/jenkins/tmp/dir/date=*/*.csv", path.localPath());
    }

    /** No pattern, nothing to narrow. */
    public void testLiteralNamePrefixIsEmptyForANonPattern() {
        assertEquals("", StoragePath.of("s3://b/data/part-0000.csv").literalNamePrefix());
        assertEquals("", StoragePath.of("s3://b/data/").literalNamePrefix());
    }

    /**
     * The prefix and the lead must compose back into something no matching key can fall outside. Checked as a
     * property rather than per case: appending the lead to the pattern prefix has to be a prefix of the pattern
     * itself, or the store is being asked for a narrower set than the pattern selects.
     * <p>
     * The lead must also stay inside one path segment. A provider may compare it against a path relative to the
     * directory it listed, which carries the platform separator, so a lead spanning a {@code /} would match
     * nothing where that separator is not {@code /} and would drop files the pattern selects. Composing into a
     * prefix of the pattern does not catch that on its own: a lead of {@code a/b} against {@code .../a/b*} is
     * still a genuine prefix.
     */
    public void testPatternPrefixPlusLeadIsAlwaysAPrefixOfThePattern() {
        List<String> patterns = List.of(
            "s3://b/data/date=2024-01-0*/*.parquet",
            "s3://b/data/year=2024/part-*.csv",
            "s3://b/data/part-*.csv",
            "s3://b/data/*.parquet",
            "s3://b/data/year=*/month=0[1-3]/*.parquet",
            "s3://b/data/f?.csv",
            "s3://b/data/a{x,y}.csv",
            "s3://b/data/" + "**" + "/*.parquet"
        );
        for (String pattern : patterns) {
            StoragePath path = StoragePath.of(pattern);
            String lead = path.literalNamePrefix();
            String composed = path.patternPrefix().toString() + lead;
            assertTrue(
                "[" + composed + "] must be a prefix of [" + pattern + "]; a longer one would exclude matching keys",
                pattern.startsWith(composed)
            );
            assertFalse("lead [" + lead + "] spans a separator, so a provider cannot compare it against one segment", lead.contains("/"));
        }
    }

    public void testIsPatternStar() {
        StoragePath path = StoragePath.of("s3://bucket/data/*.parquet");
        assertTrue(path.isPattern());
    }

    public void testIsPatternDoubleStar() {
        StoragePath path = StoragePath.of("s3://bucket/**" + "/*.parquet");
        assertTrue(path.isPattern());
    }

    public void testIsPatternQuestionMark() {
        StoragePath path = StoragePath.of("s3://bucket/file?.csv");
        assertTrue(path.isPattern());
    }

    public void testIsPatternBraces() {
        StoragePath path = StoragePath.of("s3://bucket/data.{parquet,csv}");
        assertTrue(path.isPattern());
    }

    public void testIsPatternBrackets() {
        StoragePath path = StoragePath.of("s3://bucket/file[123].parquet");
        assertTrue(path.isPattern());
    }

    public void testIsPatternLiteralPath() {
        StoragePath path = StoragePath.of("s3://bucket/data/file.parquet");
        assertFalse(path.isPattern());
    }

    public void testContainsGlobMetacharacter() {
        assertTrue(StoragePath.containsGlobMetacharacter("*.csv"));
        assertTrue(StoragePath.containsGlobMetacharacter("file?.csv"));
        assertTrue(StoragePath.containsGlobMetacharacter("{a,b}"));
        assertTrue(StoragePath.containsGlobMetacharacter("file[123]"));
        assertFalse(StoragePath.containsGlobMetacharacter("junk"));
        assertFalse(StoragePath.containsGlobMetacharacter(""));
        assertFalse(StoragePath.containsGlobMetacharacter(null));
    }

    // -- patternPrefix --

    public void testPatternPrefixStar() {
        StoragePath path = StoragePath.of("s3://b/data/2024/*.parquet");
        assertEquals("s3://b/data/2024/", path.patternPrefix().toString());
    }

    public void testPatternPrefixDoubleStar() {
        StoragePath path = StoragePath.of("s3://b/**" + "/*.parquet");
        assertEquals("s3://b/", path.patternPrefix().toString());
    }

    public void testPatternPrefixMidPath() {
        StoragePath path = StoragePath.of("s3://b/data/*/year/file.parquet");
        assertEquals("s3://b/data/", path.patternPrefix().toString());
    }

    public void testPatternPrefixLiteralReturnsSelf() {
        StoragePath path = StoragePath.of("s3://bucket/data/file.parquet");
        assertSame(path, path.patternPrefix());
    }

    // -- globPart --

    public void testGlobPartStar() {
        StoragePath path = StoragePath.of("s3://b/data/2024/*.parquet");
        assertEquals("*.parquet", path.globPart());
    }

    public void testGlobPartDoubleStar() {
        StoragePath path = StoragePath.of("s3://b/**" + "/*.parquet");
        assertEquals("**/*.parquet", path.globPart());
    }

    public void testGlobPartLiteralReturnsEmpty() {
        StoragePath path = StoragePath.of("s3://bucket/data/file.parquet");
        assertEquals("", path.globPart());
    }

    public void testGlobPartBraces() {
        StoragePath path = StoragePath.of("s3://b/data/*.{parquet,csv}");
        assertEquals("*.{parquet,csv}", path.globPart());
    }

    // -- file:// URI handling (including Windows paths) --

    public void testFileUriUnixAbsolute() {
        StoragePath path = StoragePath.of("file:///home/user/data.parquet");
        assertEquals("file", path.scheme());
        assertEquals("", path.host());
        assertEquals(-1, path.port());
        assertEquals("/home/user/data.parquet", path.path());
    }

    public void testFileUriWindowsDriveLetter() {
        StoragePath path = StoragePath.of("file://C:/Users/data/file.txt");
        assertEquals("file", path.scheme());
        assertEquals("", path.host());
        assertEquals("/C:/Users/data/file.txt", path.path());
    }

    public void testFileUriWindowsBackslashes() {
        StoragePath path = StoragePath.of("file://C:\\Users\\data\\file.txt");
        assertEquals("file", path.scheme());
        assertEquals("", path.host());
        assertEquals("/C:/Users/data/file.txt", path.path());
    }

    public void testFileUriWindowsTripleSlash() {
        StoragePath path = StoragePath.of("file:///C:/path/to/file.parquet");
        assertEquals("file", path.scheme());
        assertEquals("", path.host());
        assertEquals("/C:/path/to/file.parquet", path.path());
    }

    public void testFileUriFunction() {
        java.nio.file.Path p = PathUtils.get("/tmp/test/data.parquet");
        String uri = StoragePath.fileUri(p);
        String absPath = p.toAbsolutePath().toString().replace('\\', '/');
        if (absPath.startsWith("/") == false) {
            absPath = "/" + absPath;
        }
        assertEquals("file://" + absPath, uri);
        StoragePath sp = StoragePath.of(uri);
        assertEquals(absPath, sp.path());
    }

    // -- userInfo handling --

    public void testUserInfoAbsent() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/container/file.parquet");
        assertNull(path.userInfo());
        assertEquals("account.blob.core.windows.net", path.host());
        assertEquals("/container/file.parquet", path.path());
    }

    public void testUserInfoHadoopWasbForm() {
        StoragePath path = StoragePath.of("wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2019/file.parquet");
        assertEquals("wasbs", path.scheme());
        assertEquals("nyctlc", path.userInfo());
        assertEquals("azureopendatastorage.blob.core.windows.net", path.host());
        assertEquals("/yellow/puYear=2019/file.parquet", path.path());
    }

    public void testUserInfoEmptyNormalizedToNull() {
        StoragePath path = StoragePath.of("wasbs://@account.blob.core.windows.net/c/x");
        assertNull(path.userInfo());
        assertEquals("account.blob.core.windows.net", path.host());
    }

    public void testUserInfoToStringRoundTrip() {
        String uri = "wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/file.parquet";
        StoragePath path = StoragePath.of(uri);
        assertEquals(uri, path.toString());
    }

    public void testUserInfoLastAtIsBoundary() {
        // Per RFC 3986 the userInfo extends to the last '@' so values containing '@' (e.g. SAS tokens) survive.
        StoragePath path = StoragePath.of("s3://a@b@host/p");
        assertEquals("a@b", path.userInfo());
        assertEquals("host", path.host());
    }

    public void testUserInfoWithColonPreserved() {
        StoragePath path = StoragePath.of("https://user:pass@example.com/x");
        assertEquals("user:pass", path.userInfo());
        assertEquals("example.com", path.host());
        assertEquals("/x", path.path());
    }

    public void testUserInfoWithPort() {
        StoragePath path = StoragePath.of("https://u@host:8443/x");
        assertEquals("u", path.userInfo());
        assertEquals("host", path.host());
        assertEquals(8443, path.port());
    }

    public void testUserInfoWithIpv6() {
        StoragePath path = StoragePath.of("https://u@[::1]:8080/x");
        assertEquals("u", path.userInfo());
        assertEquals("[::1]", path.host());
        assertEquals(8080, path.port());
        assertEquals("/x", path.path());
    }

    public void testParentDirectoryPreservesUserInfo() {
        StoragePath path = StoragePath.of("wasbs://c@a.host/data/2024/file.parquet");
        StoragePath parent = path.parentDirectory();
        assertEquals("wasbs://c@a.host/data/2024", parent.toString());
        assertEquals("c", parent.userInfo());
    }

    public void testAppendPathPreservesUserInfo() {
        StoragePath path = StoragePath.of("wasbs://c@a.host/data");
        StoragePath child = path.appendPath("file.parquet");
        assertEquals("wasbs://c@a.host/data/file.parquet", child.toString());
        assertEquals("c", child.userInfo());
    }

    public void testPatternPrefixPreservesUserInfo() {
        StoragePath path = StoragePath.of("wasbs://c@a.host/data/*.parquet");
        StoragePath prefix = path.patternPrefix();
        assertEquals("wasbs://c@a.host/data/", prefix.toString());
        assertEquals("c", prefix.userInfo());
    }

    // -- HTTP/HTTPS query string and fragment stripping --

    /**
     * For http/https, '?' and '#' are structural URL delimiters. path() must return the pure
     * resource path so that isPattern(), objectName(), and callers that do extension inference
     * see only the path component. toString() must return the original URL verbatim so that
     * HTTP clients and URL-keyed caches continue to use the full presigned URL.
     */
    public void testHttpQueryStringStrippedFromPath() {
        StoragePath path = StoragePath.of("https://host/data.csv?X-Goog-Signature=abc123");
        assertEquals("/data.csv", path.path());
        assertEquals("https://host/data.csv?X-Goog-Signature=abc123", path.toString());
    }

    public void testHttpFragmentStrippedFromPath() {
        StoragePath path = StoragePath.of("https://host/data.csv#fragment");
        assertEquals("/data.csv", path.path());
        assertEquals("https://host/data.csv#fragment", path.toString());
    }

    public void testHttpQueryAndFragmentStrippedFromPath() {
        StoragePath path = StoragePath.of("https://host/data.csv?sig=xyz#frag");
        assertEquals("/data.csv", path.path());
        assertEquals("https://host/data.csv?sig=xyz#frag", path.toString());
    }

    public void testHttpDottedQueryStringStrippedBeforeExtensionScan() {
        // A dotted query value like ?v=1.2 must not confuse objectName() — the dot in "1.2"
        // must not be found by a last-dot scan over the path.
        StoragePath path = StoragePath.of("https://host/data.csv?v=1.2");
        assertEquals("/data.csv", path.path());
        assertEquals("data.csv", path.objectName());
    }

    public void testHttpDottedQueryStringDeepPath() {
        // objectName() uses path.lastIndexOf('/') — verify the strip and name extraction are
        // correct for a multi-segment path, not just a single-segment one.
        StoragePath path = StoragePath.of("https://host/path/to/data.csv?v=1.2");
        assertEquals("/path/to/data.csv", path.path());
        assertEquals("data.csv", path.objectName());
    }

    public void testHttpQueryStringIsNotAPattern() {
        // '?' in the query string must not trigger isPattern() — it is not a glob metacharacter here.
        StoragePath path = StoragePath.of("https://host/data.csv?X-Amz-Signature=abc");
        assertFalse(path.isPattern());
    }

    public void testHttpPlainGlobInPathStillAPattern() {
        // A real glob in the PATH component must still be detected as a pattern.
        StoragePath path = StoragePath.of("https://host/data/*.csv");
        assertTrue(path.isPattern());
    }

    public void testS3QuestionMarkInKeyIsStillAPattern() {
        // For object-store schemes '?' is a glob metacharacter; the stripping must NOT apply.
        StoragePath path = StoragePath.of("s3://bucket/data?.csv");
        assertTrue(path.isPattern());
    }

    public void testHttpEqualsAndHashCodeUseFullLocation() {
        // equals/hashCode must use the original location so URL-keyed caches are not corrupted.
        StoragePath a = StoragePath.of("https://host/data.csv?sig=abc");
        StoragePath b = StoragePath.of("https://host/data.csv?sig=abc");
        StoragePath c = StoragePath.of("https://host/data.csv?sig=xyz");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    public void testHttpParentDirectoryStripsQueryString() {
        // parentDirectory() operates on the clean path; the result must not carry the query string of the original URL.
        StoragePath path = StoragePath.of("https://host/dir/file.csv?sig=x");
        StoragePath parent = path.parentDirectory();
        assertEquals("https://host/dir", parent.toString());
    }

    // -- malformed authority (ARN colon) --

    public void testArnAuthorityThrowsMalformedAuthority() {
        // An ARN in the authority has colons that are not port separators; the error must say "Malformed authority", not "Invalid port".
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> StoragePath.of("s3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-ap/data/f.parquet")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("Malformed authority in location"));
        assertThat(e.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("Invalid port")));
    }

    public void testIpv6NonPortSuffixThrowsInvalidPort() {
        // IPv6 URIs can genuinely have a port; "Invalid port" is accurate for a non-numeric suffix here.
        var e = expectThrows(IllegalArgumentException.class, () -> StoragePath.of("https://[::1]:notaport/path"));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("Invalid port in location"));
    }

    // -- static objectName(String) helper --

    public void testStaticObjectNameReturnsLastSegment() {
        assertEquals("events.parquet", StoragePath.objectName("s3://my-bucket/prefix/events.parquet"));
        assertEquals("data.csv", StoragePath.objectName("https://host/a/b/data.csv"));
    }

    public void testStaticObjectNameFailsClosedOnTrailingSlash() {
        // A trailing-slash path has no object name — must return "" not the full URI.
        assertEquals("", StoragePath.objectName("s3://my-bucket/prefix/"));
        assertEquals("", StoragePath.objectName("s3://my-bucket/"));
    }

    public void testStaticObjectNameFailsClosedOnParseFailure() {
        // A malformed URI must return "" not the full string.
        assertEquals("", StoragePath.objectName("arn:aws:s3:::my-bucket/key"));
        assertEquals("", StoragePath.objectName("not-a-uri"));
    }

    public void testStaticObjectNameNullReturnsEmpty() {
        assertEquals("", StoragePath.objectName(null));
    }
}
