/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

/**
 * Unit tests for {@link FixtureUtils#injectWithEntries}. Lives in the same qa/server project as the
 * class under test so it can be a plain {@link ESTestCase} unit test (no cluster required).
 */
public class FixtureUtilsTests extends ESTestCase {

    public void testInjectWithEntriesAppendsWhenNoExistingWith() {
        String result = FixtureUtils.injectWithEntries(
            "EXTERNAL \"s3://bucket/file.csv\"",
            "\"endpoint\": \"http://localhost\", \"key\": \"abc\""
        );
        assertEquals("EXTERNAL \"s3://bucket/file.csv\" WITH { \"endpoint\": \"http://localhost\", \"key\": \"abc\" }", result);
    }

    public void testInjectWithEntriesMergesIntoExistingWith() {
        String result = FixtureUtils.injectWithEntries(
            "EXTERNAL \"s3://bucket/file.csv\" WITH { \"header_row\": false }",
            "\"endpoint\": \"http://localhost\""
        );
        assertEquals("EXTERNAL \"s3://bucket/file.csv\" WITH { \"endpoint\": \"http://localhost\", \"header_row\": false }", result);
    }

    public void testInjectWithEntriesIgnoresWithInsideQuotedPath() {
        String result = FixtureUtils.injectWithEntries("EXTERNAL \"s3://bucket/WITH_data.csv\"", "\"endpoint\": \"http://localhost\"");
        assertEquals("EXTERNAL \"s3://bucket/WITH_data.csv\" WITH { \"endpoint\": \"http://localhost\" }", result);
    }

    public void testInjectWithEntriesIgnoresQuotedWithKeyword() {
        String result = FixtureUtils.injectWithEntries("EXTERNAL \"s3://bucket/WITH.csv\"", "\"key\": \"val\"");
        assertEquals("EXTERNAL \"s3://bucket/WITH.csv\" WITH { \"key\": \"val\" }", result);
    }

    public void testInjectWithEntriesCaseInsensitiveWith() {
        String result = FixtureUtils.injectWithEntries("EXTERNAL \"s3://b/f.csv\" with { \"existing\": true }", "\"new_key\": \"v\"");
        assertEquals("EXTERNAL \"s3://b/f.csv\" with { \"new_key\": \"v\", \"existing\": true }", result);
    }

    public void testInjectWithEntriesEmptyEntriesNoExistingWith() {
        String result = FixtureUtils.injectWithEntries("EXTERNAL \"s3://b/f.csv\"", "");
        assertEquals("EXTERNAL \"s3://b/f.csv\" WITH { }", result);
    }

    public void testInjectWithEntriesEmptyEntriesWithExistingWith() {
        String result = FixtureUtils.injectWithEntries("EXTERNAL \"s3://b/f.csv\" WITH { \"header_row\": false }", "");
        assertEquals("EXTERNAL \"s3://b/f.csv\" WITH { \"header_row\": false }", result);
    }
}
