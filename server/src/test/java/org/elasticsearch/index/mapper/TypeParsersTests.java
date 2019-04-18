/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
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
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TypeParsersTests extends ESTestCase {

    private static final IndexMetaData EMPTY_INDEX_METADATA = IndexMetaData.builder("")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(0).build();
    private static final IndexSettings indexSettings = new IndexSettings(EMPTY_INDEX_METADATA, Settings.EMPTY);

    public void testParseTextFieldCheckAnalyzerAnalysisMode() {
        TextFieldMapper.Builder builder = new TextFieldMapper.Builder("textField");
        Map<String, Object> fieldNode = new HashMap<String, Object>();
        fieldNode.put("analyzer", "my_analyzer");
        Mapper.TypeParser.ParserContext parserContext = mock(Mapper.TypeParser.ParserContext.class);

        // check AnalysisMode.ALL works
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put("my_analyzer",
                new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", AnalysisMode.ALL)));

        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(indexSettings, new NamedAnalyzer("default", AnalyzerScope.INDEX, null), null,
                null, analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext);

        // check that "analyzer" set to something that only supports AnalysisMode.SEARCH_TIME or AnalysisMode.INDEX_TIME is blocked
        AnalysisMode mode = randomFrom(AnalysisMode.SEARCH_TIME, AnalysisMode.INDEX_TIME);
        analyzers = new HashMap<>();
        analyzers.put("my_analyzer", new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX,
                createAnalyzerWithMode("my_analyzer", mode)));
        indexAnalyzers = new IndexAnalyzers(indexSettings, new NamedAnalyzer("default", AnalyzerScope.INDEX, null), null, null, analyzers,
                Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        MapperException ex = expectThrows(MapperException.class,
                () -> TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext));
        assertEquals("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run in all mode.",
                ex.getMessage());
    }

    public void testParseTextFieldCheckSearchAnalyzerAnalysisMode() {
        TextFieldMapper.Builder builder = new TextFieldMapper.Builder("textField");
        for (String settingToTest : new String[] { "search_analyzer", "search_quote_analyzer" }) {
            Map<String, Object> fieldNode = new HashMap<String, Object>();
            fieldNode.put(settingToTest, "my_analyzer");
            fieldNode.put("analyzer", "standard");
            if (settingToTest.equals("search_quote_analyzer")) {
                fieldNode.put("search_analyzer", "standard");
            }
            Mapper.TypeParser.ParserContext parserContext = mock(Mapper.TypeParser.ParserContext.class);

            // check AnalysisMode.ALL and AnalysisMode.SEARCH_TIME works
            Map<String, NamedAnalyzer> analyzers = new HashMap<>();
            AnalysisMode mode = randomFrom(AnalysisMode.ALL, AnalysisMode.SEARCH_TIME);
            analyzers.put("my_analyzer",
                    new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", mode)));
            analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));

            IndexAnalyzers indexAnalyzers = new IndexAnalyzers(indexSettings, new NamedAnalyzer("default", AnalyzerScope.INDEX, null), null,
                    null, analyzers, Collections.emptyMap(), Collections.emptyMap());
            when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
            TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext);

            // check that "analyzer" set to AnalysisMode.INDEX_TIME is blocked
            mode = AnalysisMode.INDEX_TIME;
            analyzers = new HashMap<>();
            analyzers.put("my_analyzer",
                    new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", mode)));
            analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
            indexAnalyzers = new IndexAnalyzers(indexSettings, new NamedAnalyzer("default", AnalyzerScope.INDEX, null), null, null,
                    analyzers, Collections.emptyMap(), Collections.emptyMap());
            when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
            MapperException ex = expectThrows(MapperException.class,
                    () -> TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext));
            assertEquals("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run in search time mode.",
                    ex.getMessage());
        }
    }

    public void testParseTextFieldCheckAnalyzerWithSearchAnalyzerAnalysisMode() {
        TextFieldMapper.Builder builder = new TextFieldMapper.Builder("textField");
        Map<String, Object> fieldNode = new HashMap<String, Object>();
        fieldNode.put("analyzer", "my_analyzer");
        Mapper.TypeParser.ParserContext parserContext = mock(Mapper.TypeParser.ParserContext.class);

        // check that "analyzer" set to AnalysisMode.INDEX_TIME is blocked if there is no search analyzer
        AnalysisMode mode = AnalysisMode.INDEX_TIME;
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put("my_analyzer",
                new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", mode)));
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(indexSettings, new NamedAnalyzer("default", AnalyzerScope.INDEX, null), null,
                null, analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        MapperException ex = expectThrows(MapperException.class,
                () -> TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext));
        assertEquals("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run in all mode.",
                ex.getMessage());

        // check AnalysisMode.INDEX_TIME is okay if search analyzer is also set
        fieldNode.put("search_analyzer", "standard");
        analyzers = new HashMap<>();
        mode = randomFrom(AnalysisMode.ALL, AnalysisMode.INDEX_TIME);
        analyzers.put("my_analyzer",
                new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", mode)));
        analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));

        indexAnalyzers = new IndexAnalyzers(indexSettings, new NamedAnalyzer("default", AnalyzerScope.INDEX, null), null, null, analyzers,
                Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext);
    }

    private Analyzer createAnalyzerWithMode(String name, AnalysisMode mode) {
        TokenFilterFactory tokenFilter = new AbstractTokenFilterFactory(indexSettings, name, Settings.EMPTY) {
            @Override
            public AnalysisMode getAnalysisMode() {
                return mode;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return null;
            }
        };
        return new CustomAnalyzer("tokenizerName", null, new CharFilterFactory[0],
                new TokenFilterFactory[] { tokenFilter  });
    }
}
