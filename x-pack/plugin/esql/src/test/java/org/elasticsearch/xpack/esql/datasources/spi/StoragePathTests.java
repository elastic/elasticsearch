/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

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

    public void testFileUriFunctionOnUnix() {
        java.nio.file.Path p = java.nio.file.Path.of("/tmp/test/data.parquet");
        String uri = StoragePath.fileUri(p);
        assertEquals("file:///tmp/test/data.parquet", uri);
        StoragePath sp = StoragePath.of(uri);
        assertEquals("/tmp/test/data.parquet", sp.path());
    }
}
