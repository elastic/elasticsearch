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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;

abstract class AnalyzerUnmappedTestBase extends ESTestCase {

    static TestAnalyzer test() {
        return analyzer().addEmployees("test");
    }

    static String setUnmappedLoad(String query) {
        return "SET unmapped_fields=\"load\"; " + query;
    }

    static String setUnmappedNullify(String query) {
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    static EsField keywordField(String name) {
        return new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
    }

    static IndexResolution lookupIndexWithOverlappingFields() {
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

    static IndexResolution keywordLanguagesLookup() {
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

    static TestAnalyzer partialMappingTest() {
        return analyzer().addIndex("partial_mapping_sample_data", "mapping-partial_mapping_sample_data.json")
            .addLookupIndex("partial_message_types_lookup", "mapping-partial_message_types_lookup.json");
    }

    static IndexResolution messageLookupIndex() {
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
