/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TextFieldAnalyzerModeTests extends ESTestCase {

    private static Map<String, NamedAnalyzer> defaultAnalyzers() {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put(DEFAULT_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        return analyzers;
    }

    private static final IndexMetadata EMPTY_INDEX_METADATA = IndexMetadata.builder("")
        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
        .numberOfShards(1)
        .numberOfReplicas(0)
        .build();

    private Analyzer createAnalyzerWithMode(AnalysisMode mode) {
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
        return new CustomAnalyzer(null, new CharFilterFactory[0], new TokenFilterFactory[] { tokenFilter });
    }

    public void testParseTextFieldCheckAnalyzerAnalysisMode() {

        Map<String, Object> fieldNode = new HashMap<>();
        fieldNode.put("analyzer", "my_analyzer");
        MappingParserContext parserContext = mock(MappingParserContext.class);
        when(parserContext.indexVersionCreated()).thenReturn(IndexVersion.CURRENT);

        // check AnalysisMode.ALL works
        Map<String, NamedAnalyzer> analyzers = defaultAnalyzers();
        analyzers.put("my_analyzer", new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(AnalysisMode.ALL)));

        IndexAnalyzers indexAnalyzers = IndexAnalyzers.of(analyzers);
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);

        TextFieldMapper.PARSER.parse("field", fieldNode, parserContext);

        // check that "analyzer" set to something that only supports AnalysisMode.SEARCH_TIME or AnalysisMode.INDEX_TIME is blocked
        AnalysisMode mode = randomFrom(AnalysisMode.SEARCH_TIME, AnalysisMode.INDEX_TIME);
        analyzers = defaultAnalyzers();
        analyzers.put("my_analyzer", new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(mode)));
        indexAnalyzers = IndexAnalyzers.of(analyzers);
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        fieldNode.put("analyzer", "my_analyzer");
        MapperException ex = expectThrows(MapperException.class, () -> { TextFieldMapper.PARSER.parse("name", fieldNode, parserContext); });
        assertThat(
            ex.getMessage(),
            containsString("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run")
        );
    }

    public void testParseTextFieldCheckSearchAnalyzerAnalysisMode() {

        for (String settingToTest : new String[] { "search_analyzer", "search_quote_analyzer" }) {
            Map<String, Object> fieldNode = new HashMap<>();
            fieldNode.put(settingToTest, "my_analyzer");
            fieldNode.put("analyzer", "standard");
            if (settingToTest.equals("search_quote_analyzer")) {
                fieldNode.put("search_analyzer", "standard");
            }
            MappingParserContext parserContext = mock(MappingParserContext.class);
            when(parserContext.indexVersionCreated()).thenReturn(IndexVersion.CURRENT);

            // check AnalysisMode.ALL and AnalysisMode.SEARCH_TIME works
            Map<String, NamedAnalyzer> analyzers = defaultAnalyzers();
            AnalysisMode mode = randomFrom(AnalysisMode.ALL, AnalysisMode.SEARCH_TIME);
            analyzers.put("my_analyzer", new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(mode)));
            analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));

            IndexAnalyzers indexAnalyzers = IndexAnalyzers.of(analyzers);
            when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
            TextFieldMapper.PARSER.parse("textField", fieldNode, parserContext);

            // check that "analyzer" set to AnalysisMode.INDEX_TIME is blocked
            mode = AnalysisMode.INDEX_TIME;
            analyzers = defaultAnalyzers();
            analyzers.put("my_analyzer", new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(mode)));
            analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
            indexAnalyzers = IndexAnalyzers.of(analyzers);
            when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
            fieldNode.clear();
            fieldNode.put(settingToTest, "my_analyzer");
            fieldNode.put("analyzer", "standard");
            if (settingToTest.equals("search_quote_analyzer")) {
                fieldNode.put("search_analyzer", "standard");
            }
            MapperException ex = expectThrows(
                MapperException.class,
                () -> { TextFieldMapper.PARSER.parse("field", fieldNode, parserContext); }
            );
            assertEquals(
                "analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run in search time mode.",
                ex.getMessage()
            );
        }
    }

    public void testParseTextFieldCheckAnalyzerWithSearchAnalyzerAnalysisMode() {

        Map<String, Object> fieldNode = new HashMap<>();
        fieldNode.put("analyzer", "my_analyzer");
        MappingParserContext parserContext = mock(MappingParserContext.class);
        when(parserContext.indexVersionCreated()).thenReturn(IndexVersion.CURRENT);

        // check that "analyzer" set to AnalysisMode.INDEX_TIME is blocked if there is no search analyzer
        AnalysisMode mode = AnalysisMode.INDEX_TIME;
        Map<String, NamedAnalyzer> analyzers = defaultAnalyzers();
        analyzers.put("my_analyzer", new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(mode)));
        IndexAnalyzers indexAnalyzers = IndexAnalyzers.of(analyzers);
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        MapperException ex = expectThrows(
            MapperException.class,
            () -> { TextFieldMapper.PARSER.parse("field", fieldNode, parserContext); }
        );
        assertThat(
            ex.getMessage(),
            containsString("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run")
        );

        // check AnalysisMode.INDEX_TIME is okay if search analyzer is also set
        fieldNode.put("analyzer", "my_analyzer");
        fieldNode.put("search_analyzer", "standard");
        analyzers = defaultAnalyzers();
        mode = randomFrom(AnalysisMode.ALL, AnalysisMode.INDEX_TIME);
        analyzers.put("my_analyzer", new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(mode)));
        analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));

        indexAnalyzers = IndexAnalyzers.of(analyzers);
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        TextFieldMapper.PARSER.parse("field", fieldNode, parserContext);
    }

}
