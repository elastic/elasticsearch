/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.test.ESTestCase;

public class NamedAnalyzerTests extends ESTestCase {

    public void testCheckAllowedInMode() {
        try (
            NamedAnalyzer testAnalyzer = new NamedAnalyzer(
                "my_analyzer",
                AnalyzerScope.INDEX,
                createAnalyzerWithMode("my_analyzer", AnalysisMode.INDEX_TIME),
                Integer.MIN_VALUE
            )
        ) {
            testAnalyzer.checkAllowedInMode(AnalysisMode.INDEX_TIME);
            MapperException ex = expectThrows(MapperException.class, () -> testAnalyzer.checkAllowedInMode(AnalysisMode.SEARCH_TIME));
            assertEquals(
                "analyzer [my_analyzer] contains filters [my_analyzer] that are not allowed to run in search time mode.",
                ex.getMessage()
            );
            ex = expectThrows(MapperException.class, () -> testAnalyzer.checkAllowedInMode(AnalysisMode.ALL));
            assertEquals("analyzer [my_analyzer] contains filters [my_analyzer] that are not allowed to run in all mode.", ex.getMessage());
        }

        try (
            NamedAnalyzer testAnalyzer = new NamedAnalyzer(
                "my_analyzer",
                AnalyzerScope.INDEX,
                createAnalyzerWithMode("my_analyzer", AnalysisMode.SEARCH_TIME),
                Integer.MIN_VALUE
            )
        ) {
            testAnalyzer.checkAllowedInMode(AnalysisMode.SEARCH_TIME);
            MapperException ex = expectThrows(MapperException.class, () -> testAnalyzer.checkAllowedInMode(AnalysisMode.INDEX_TIME));
            assertEquals(
                "analyzer [my_analyzer] contains filters [my_analyzer] that are not allowed to run in index time mode.",
                ex.getMessage()
            );
            ex = expectThrows(MapperException.class, () -> testAnalyzer.checkAllowedInMode(AnalysisMode.ALL));
            assertEquals("analyzer [my_analyzer] contains filters [my_analyzer] that are not allowed to run in all mode.", ex.getMessage());
        }

        try (
            NamedAnalyzer testAnalyzer = new NamedAnalyzer(
                "my_analyzer",
                AnalyzerScope.INDEX,
                createAnalyzerWithMode("my_analyzer", AnalysisMode.ALL),
                Integer.MIN_VALUE
            )
        ) {
            testAnalyzer.checkAllowedInMode(AnalysisMode.ALL);
            testAnalyzer.checkAllowedInMode(AnalysisMode.INDEX_TIME);
            testAnalyzer.checkAllowedInMode(AnalysisMode.SEARCH_TIME);
        }
    }

    private Analyzer createAnalyzerWithMode(String name, AnalysisMode mode) {
        TokenFilterFactory tokenFilter = new TokenFilterFactory() {

            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return null;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return mode;
            }
        };
        TokenFilterFactory[] tokenfilters = new TokenFilterFactory[] { tokenFilter };
        CharFilterFactory[] charFilters = new CharFilterFactory[0];
        if (mode == AnalysisMode.SEARCH_TIME && randomBoolean()) {
            AnalyzerComponents components = new AnalyzerComponents(null, charFilters, tokenfilters);
            // sometimes also return reloadable custom analyzer
            return new ReloadableCustomAnalyzer(components, TextFieldMapper.Defaults.POSITION_INCREMENT_GAP, -1);
        }
        return new CustomAnalyzer(null, charFilters, tokenfilters);
    }
}
