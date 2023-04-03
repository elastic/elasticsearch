/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TextFieldAnalyzerModeTests extends MapperServiceTestCase {

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of(
                "all",
                createAnalyzerWithMode("all", AnalysisMode.ALL),
                "index",
                createAnalyzerWithMode("index", AnalysisMode.INDEX_TIME),
                "search",
                createAnalyzerWithMode("search", AnalysisMode.SEARCH_TIME)
            )
        );
    }

    private static NamedAnalyzer createAnalyzerWithMode(String name, AnalysisMode mode) {
        TokenFilterFactory tokenFilter = new AbstractTokenFilterFactory("my_analyzer", Settings.EMPTY) {
            @Override
            public AnalysisMode getAnalysisMode() {
                return mode;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return null;
            }
        };
        return new NamedAnalyzer(
            name,
            AnalyzerScope.INDEX,
            new CustomAnalyzer(null, new CharFilterFactory[0], new TokenFilterFactory[] { tokenFilter })
        );
    }

    public void testParseTextFieldCheckAnalyzerAnalysisMode() throws IOException {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
              "text1" : { "type" : "text", "analyzer" : "all" },
              "text2" : { "type" : "text", "analyzer" : "index", "search_analyzer" : "search" }
            }}}
            """);

        assertThat(mapperService.mappingLookup().indexAnalyzer("text1", field -> null).name(), equalTo("all"));
        assertThat(mapperService.mappingLookup().indexAnalyzer("text2", field -> null).name(), equalTo("index"));

        Exception e = expectThrows(MapperException.class, () -> createMapperService("""
            { "_doc" : { "properties" : { "text" : { "type" : "text", "analyzer" : "search" } } } }
            """));
        assertThat(e.getMessage(), containsString("analyzer [search] contains filters [my_analyzer] that are not allowed to run"));

        Exception e2 = expectThrows(MapperException.class, () -> createMapperService("""
            { "_doc" : { "properties" : { "text" : { "type" : "text", "analyzer" : "index" } } } }
            """));
        assertThat(e2.getMessage(), containsString("analyzer [index] contains filters [my_analyzer] that are not allowed to run"));

        Exception e3 = expectThrows(MapperException.class, () -> createMapperService("""
            { "_doc" : { "properties" : { "text" : { "type" : "text", "analyzer" : "index", "search_quote_analyzer" : "search" } } } }
            """));
        assertThat(e.getMessage(), containsString("analyzer [search] contains filters [my_analyzer] that are not allowed to run"));
    }

}
