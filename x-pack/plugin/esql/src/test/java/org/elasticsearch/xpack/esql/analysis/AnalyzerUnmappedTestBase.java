/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;

/**
 * Shared infrastructure for the AnalyzerUnmapped_* focused test classes.
 */
abstract class AnalyzerUnmappedTestBase extends ESTestCase {

    /** Returns a TestAnalyzer pre-loaded with the employees ("test") index. */
    protected static TestAnalyzer test() {
        return analyzer().addEmployees("test");
    }

    /**
     * Wraps a query with {@code SET unmapped_fields="load";}.
     * Skips the test if the OPTIONAL_FIELDS_V5 capability is not available.
     */
    protected static String setUnmappedLoad(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());
        return "SET unmapped_fields=\"load\"; " + query;
    }

    /**
     * Wraps a query with {@code SET unmapped_fields="nullify";}.
     * Skips the test if the OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW capability is not available.
     */
    protected static String setUnmappedNullify(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    protected static EsField keywordField(String name) {
        return new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
    }

    /**
     * Builds the {@code custom_lookup} index used for non-key field semantic tests.
     * Fields:
     * <ul>
     *   <li>{@code language_code} (INTEGER) — join key</li>
     *   <li>{@code salary} (KEYWORD) — same name as primary's INTEGER salary; lookup type wins</li>
     *   <li>{@code lookup_only} (KEYWORD) — present only in lookup, absent from primary</li>
     * </ul>
     */
    protected static IndexResolution lookupIndexWithOverlappingFields() {
        Map<String, EsField> mapping = Map.of(
            "language_code",
            new EsField("language_code", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "salary",
            keywordField("salary"),
            "lookup_only",
            keywordField("lookup_only")
        );
        return IndexResolution.valid(new EsIndex("custom_lookup", mapping, Map.of("custom_lookup", IndexMode.LOOKUP), Map.of(), Map.of()));
    }

    /**
     * Builds a lookup index with {@code language_code} mapped as KEYWORD.
     * Used to test the unmapped-left / mapped-right case where types are compatible with load mode (PUK is always keyword).
     */
    protected static IndexResolution keywordLanguagesLookup() {
        return IndexResolution.valid(
            new EsIndex(
                "keyword_languages_lookup",
                Map.of("language_code", keywordField("language_code"), "language_name", keywordField("language_name")),
                Map.of("keyword_languages_lookup", IndexMode.LOOKUP),
                Map.of(),
                Map.of()
            )
        );
    }

    /**
     * Builds the {@code message_lookup} index used for KEEP-before-join tests.
     * The primary employees index has no {@code message} field; this lookup does.
     */
    protected static IndexResolution messageLookupIndex() {
        return IndexResolution.valid(
            new EsIndex(
                "message_lookup",
                Map.of("message", keywordField("message"), "type", keywordField("type")),
                Map.of("message_lookup", IndexMode.LOOKUP),
                Map.of(),
                Map.of()
            )
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }
}
