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
        // Nothing to inject and no existing WITH: return the input unchanged rather than
        // synthesizing an empty WITH { } that the parser may reject.
        String result = FixtureUtils.injectWithEntries("EXTERNAL \"s3://b/f.csv\"", "");
        assertEquals("EXTERNAL \"s3://b/f.csv\"", result);
    }

    public void testInjectWithEntriesEmptyEntriesWithExistingWith() {
        String result = FixtureUtils.injectWithEntries("EXTERNAL \"s3://b/f.csv\" WITH { \"header_row\": false }", "");
        assertEquals("EXTERNAL \"s3://b/f.csv\" WITH { \"header_row\": false }", result);
    }

    public void testInjectWithEntriesIgnoresWithAsIdentifierPrefix() {
        // "with_credentials" has WITH as a prefix but is part of a longer identifier — must not match.
        String result = FixtureUtils.injectWithEntries(
            "EXTERNAL \"s3://b/f.csv\" with_credentials = 1",
            "\"endpoint\": \"http://localhost\""
        );
        assertEquals("EXTERNAL \"s3://b/f.csv\" with_credentials = 1 WITH { \"endpoint\": \"http://localhost\" }", result);
    }

    public void testInjectForEachExternalLeadingExternalMatchesSingleInjection() {
        // A leading standalone EXTERNAL must behave identically to injectWithEntries.
        String result = FixtureUtils.injectWithEntriesForEachExternal(
            "EXTERNAL \"s3://b/f.csv\" | WHERE x > 0",
            "\"endpoint\": \"http://localhost\""
        );
        assertEquals("EXTERNAL \"s3://b/f.csv\" WITH { \"endpoint\": \"http://localhost\" } | WHERE x > 0", result);
    }

    public void testInjectForEachExternalNestedInFromSubquery() {
        // EXTERNAL nested inside a FROM subquery (the case that produced auth errors before the fix).
        String result = FixtureUtils.injectWithEntriesForEachExternal("FROM employees, (EXTERNAL \"s3://b/f.csv\")", "\"key\": \"abc\"");
        assertEquals("FROM employees, (EXTERNAL \"s3://b/f.csv\" WITH { \"key\": \"abc\" })", result);
    }

    public void testInjectForEachExternalNestedWithInnerPipe() {
        // The pipe lives inside the subquery; injection must land before it, not be skipped.
        String result = FixtureUtils.injectWithEntriesForEachExternal(
            "FROM (EXTERNAL \"s3://b/f.csv\" | WHERE salary > 1 | KEEP emp_no)",
            "\"key\": \"abc\""
        );
        assertEquals("FROM (EXTERNAL \"s3://b/f.csv\" WITH { \"key\": \"abc\" } | WHERE salary > 1 | KEEP emp_no)", result);
    }

    public void testInjectForEachExternalMultipleSources() {
        // Every EXTERNAL gets credentials, not just the first.
        String result = FixtureUtils.injectWithEntriesForEachExternal(
            "FROM (EXTERNAL \"s3://b/a.csv\"), (EXTERNAL \"s3://b/b.csv\") | STATS c = COUNT(*)",
            "\"key\": \"abc\""
        );
        assertEquals(
            "FROM (EXTERNAL \"s3://b/a.csv\" WITH { \"key\": \"abc\" }), "
                + "(EXTERNAL \"s3://b/b.csv\" WITH { \"key\": \"abc\" }) | STATS c = COUNT(*)",
            result
        );
    }

    public void testInjectForEachExternalMergesIntoExistingWith() {
        String result = FixtureUtils.injectWithEntriesForEachExternal(
            "FROM (EXTERNAL \"s3://b/f.csv\" WITH { \"header_row\": false } | KEEP x)",
            "\"endpoint\": \"http://localhost\""
        );
        assertEquals(
            "FROM (EXTERNAL \"s3://b/f.csv\" WITH { \"endpoint\": \"http://localhost\", \"header_row\": false } | KEEP x)",
            result
        );
    }

    public void testInjectForEachExternalIgnoresExternalInsideQuotedPath() {
        // The word EXTERNAL inside the quoted source path must not be treated as a command.
        String result = FixtureUtils.injectWithEntriesForEachExternal("FROM (EXTERNAL \"s3://b/EXTERNAL.csv\")", "\"key\": \"v\"");
        assertEquals("FROM (EXTERNAL \"s3://b/EXTERNAL.csv\" WITH { \"key\": \"v\" })", result);
    }

    public void testInjectForEachExternalEmptyEntriesNoChange() {
        String query = "FROM employees, (EXTERNAL \"s3://b/f.csv\")";
        assertEquals(query, FixtureUtils.injectWithEntriesForEachExternal(query, ""));
    }

    public void testInjectForEachExternalNoExternalIsNoOp() {
        // A query with no EXTERNAL command is returned unchanged — this is what makes it safe for
        // isExternalQuery to over-approximate (e.g. return true based on a capability flag).
        String query = "FROM employees | WHERE x > 0";
        assertEquals(query, FixtureUtils.injectWithEntriesForEachExternal(query, "\"key\": \"abc\""));
    }
}
