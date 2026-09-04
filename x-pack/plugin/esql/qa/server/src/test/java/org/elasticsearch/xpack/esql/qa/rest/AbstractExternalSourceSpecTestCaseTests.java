/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Unit tests for path resolution helpers in {@link AbstractExternalSourceSpecTestCase}.
 * The interesting case is glob path resolution on Windows: {@link Path#resolve(String)}
 * rejects {@code *} on NTFS because it is a reserved filename character, so multi-file
 * fixture paths like {@code multifile/*.csv} must not be round-tripped through
 * {@link Path}.
 */
public class AbstractExternalSourceSpecTestCaseTests extends ESTestCase {

    public void testResolveLocalUriHandlesLiteralPath() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "standalone/employees.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected path tail in URI, was: " + uri, uri.endsWith("/standalone/employees.csv"));
    }

    public void testResolveLocalUriHandlesGlobInLeafSegment() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "multifile/*.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected glob to be preserved in URI, was: " + uri, uri.endsWith("/multifile/*.csv"));
    }

    public void testResolveLocalUriHandlesDoubleGlob() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "hive-partitioned/**/*.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected glob to be preserved in URI, was: " + uri, uri.endsWith("/hive-partitioned/**/*.csv"));
    }

    public void testResolveLocalUriHandlesGlobInFirstSegment() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "*.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected glob to be preserved in URI, was: " + uri, uri.endsWith("/*.csv"));
    }

    public void testResolveLocalUriHandlesQuestionMarkGlob() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "multifile/file?.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected glob to be preserved in URI, was: " + uri, uri.endsWith("/multifile/file?.csv"));
    }

    public void testInjectTrimSpacesAddsToNullWith() {
        assertEquals("{\"trim_spaces\": true}", AbstractExternalSourceSpecTestCase.injectTrimSpaces(null));
    }

    public void testInjectTrimSpacesAddsToEmptyObject() {
        assertEquals("{\"trim_spaces\": true}", AbstractExternalSourceSpecTestCase.injectTrimSpaces("{}"));
        assertEquals("{\"trim_spaces\": true}", AbstractExternalSourceSpecTestCase.injectTrimSpaces("{ }"));
    }

    public void testInjectTrimSpacesMergesIntoExistingOptions() {
        assertEquals(
            "{\"header_row\": false, \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{\"header_row\": false}")
        );
    }

    public void testInjectTrimSpacesLeavesExplicitTrimSpacesUntouched() {
        String withJson = "{\"trim_spaces\": false}";
        assertEquals(withJson, AbstractExternalSourceSpecTestCase.injectTrimSpaces(withJson));
    }

    public void testInjectTrimSpacesDoesNotFalseMatchAValue() {
        // "trim_spaces" appears only as a value here, so the injection must still fire.
        assertEquals(
            "{\"null_value\": \"trim_spaces\", \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{\"null_value\": \"trim_spaces\"}")
        );
    }

    /**
     * A declared schema is nested objects deep, so the trailing entry of a csv/tsv directive can be an OBJECT.
     * The injection walks back from the last brace, which is the outermost closer for a parser-guaranteed
     * single object -- so the key must land beside the declaration, never inside it, where the dataset PUT's
     * mappings parser would reject it as an unknown mappings field.
     */
    public void testInjectTrimSpacesLandsOutsideATrailingNestedObject() {
        assertEquals(
            "{\"mappings\": {\"properties\": {\"a\": {\"type\": \"keyword\"}}}, \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{\"mappings\": {\"properties\": {\"a\": {\"type\": \"keyword\"}}}}")
        );
    }

    /**
     * A declared column may be NAMED trim_spaces. Deciding the already-set check by matching the raw text would
     * see that nested key and skip the injection, reading the column-aligned csv/tsv fixtures untrimmed -- values
     * wrong, with nothing pointing at the cause. The setting is absent here, so the injection must fire.
     */
    public void testInjectTrimSpacesIgnoresASameNamedDeclaredColumn() {
        assertEquals(
            "{\"mappings\": {\"properties\": {\"trim_spaces\": {\"type\": \"keyword\"}}}, \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces(
                "{\"mappings\": {\"properties\": {\"trim_spaces\": {\"type\": \"keyword\"}}}}"
            )
        );
    }

    /** An explicitly-set trim_spaces SETTING is still left untouched, alongside a declared schema. */
    public void testInjectTrimSpacesLeavesAnExplicitSettingUntouchedBesideADeclaration() {
        String withJson = "{\"trim_spaces\": false, \"mappings\": {\"dynamic\": \"false\"}}";
        assertEquals(withJson, AbstractExternalSourceSpecTestCase.injectTrimSpaces(withJson));
    }
}
