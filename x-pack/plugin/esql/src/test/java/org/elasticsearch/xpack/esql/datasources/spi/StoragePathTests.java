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
}
