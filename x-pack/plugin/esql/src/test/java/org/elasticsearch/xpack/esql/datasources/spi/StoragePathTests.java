/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

@SuppressWarnings("RegexpMultiline")
public class StoragePathTests extends ESTestCase {

    // -- isPattern --

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
}
