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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
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
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TypeParsersTests extends ESTestCase {

    private static final IndexMetadata EMPTY_INDEX_METADATA = IndexMetadata.builder("")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(0).build();
    private static final IndexSettings indexSettings = new IndexSettings(EMPTY_INDEX_METADATA, Settings.EMPTY);

    public void testParseTextFieldCheckAnalyzerAnalysisMode() {
        TextFieldMapper.Builder builder = new TextFieldMapper.Builder("textField");
        Map<String, Object> fieldNode = new HashMap<String, Object>();
        fieldNode.put("analyzer", "my_analyzer");
        Mapper.TypeParser.ParserContext parserContext = mock(Mapper.TypeParser.ParserContext.class);

        // check AnalysisMode.ALL works
        Map<String, NamedAnalyzer> analyzers = defaultAnalyzers();
        analyzers.put("my_analyzer",
                new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", AnalysisMode.ALL)));

        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext);

        // check that "analyzer" set to something that only supports AnalysisMode.SEARCH_TIME or AnalysisMode.INDEX_TIME is blocked
        AnalysisMode mode = randomFrom(AnalysisMode.SEARCH_TIME, AnalysisMode.INDEX_TIME);
        analyzers = defaultAnalyzers();
        analyzers.put("my_analyzer", new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX,
                createAnalyzerWithMode("my_analyzer", mode)));
        indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        MapperException ex = expectThrows(MapperException.class,
                () -> TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext));
        assertEquals("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run in all mode.",
                ex.getMessage());
    }

    private static Map<String, NamedAnalyzer> defaultAnalyzers() {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put(DEFAULT_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        analyzers.put(DEFAULT_SEARCH_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        analyzers.put(DEFAULT_SEARCH_QUOTED_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        return analyzers;
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
            Map<String, NamedAnalyzer> analyzers = defaultAnalyzers();
            AnalysisMode mode = randomFrom(AnalysisMode.ALL, AnalysisMode.SEARCH_TIME);
            analyzers.put("my_analyzer",
                    new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", mode)));
            analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));

            IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
            when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
            TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext);

            // check that "analyzer" set to AnalysisMode.INDEX_TIME is blocked
            mode = AnalysisMode.INDEX_TIME;
            analyzers = defaultAnalyzers();
            analyzers.put("my_analyzer",
                    new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", mode)));
            analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
            indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
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
        Map<String, NamedAnalyzer> analyzers = defaultAnalyzers();
        analyzers.put("my_analyzer",
                new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", mode)));
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        MapperException ex = expectThrows(MapperException.class,
                () -> TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext));
        assertEquals("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run in all mode.",
                ex.getMessage());

        // check AnalysisMode.INDEX_TIME is okay if search analyzer is also set
        fieldNode.put("search_analyzer", "standard");
        analyzers = defaultAnalyzers();
        mode = randomFrom(AnalysisMode.ALL, AnalysisMode.INDEX_TIME);
        analyzers.put("my_analyzer",
                new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode("my_analyzer", mode)));
        analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));

        indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        TypeParsers.parseTextField(builder, "name", new HashMap<>(fieldNode), parserContext);
    }

    public void testMultiFieldWithinMultiField() throws IOException {
        TextFieldMapper.Builder builder = new TextFieldMapper.Builder("textField");

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .field("type", "keyword")
            .startObject("fields")
                .startObject("sub-field")
                    .field("type", "keyword")
                    .startObject("fields")
                        .startObject("sub-sub-field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
        .endObject();

        Mapper.TypeParser typeParser = new KeywordFieldMapper.TypeParser();

        // For indices created prior to 8.0, we should only emit a warning and not fail parsing.
        Map<String, Object> fieldNode = XContentHelper.convertToMap(
            BytesReference.bytes(mapping), true, mapping.contentType()).v2();

        Version olderVersion = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        Mapper.TypeParser.ParserContext olderContext = new Mapper.TypeParser.ParserContext(
            null, null, type -> typeParser, olderVersion, null);

        TypeParsers.parseField(builder, "some-field", fieldNode, olderContext);
        assertWarnings("At least one multi-field, [sub-field], " +
            "was encountered that itself contains a multi-field. Defining multi-fields within a multi-field is deprecated " +
            "and is not supported for indices created in 8.0 and later. To migrate the mappings, all instances of [fields] " +
            "that occur within a [fields] block should be removed from the mappings, either by flattening the chained " +
            "[fields] blocks into a single level, or switching to [copy_to] if appropriate.");

        // For indices created in 8.0 or later, we should throw an error.
        Map<String, Object> fieldNodeCopy = XContentHelper.convertToMap(
            BytesReference.bytes(mapping), true, mapping.contentType()).v2();

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.CURRENT);
        Mapper.TypeParser.ParserContext context = new Mapper.TypeParser.ParserContext(
            null, null, type -> typeParser, version, null);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> TypeParsers.parseField(builder, "some-field", fieldNodeCopy, context));
        assertThat(e.getMessage(), equalTo("Encountered a multi-field [sub-field] which itself contains a " +
            "multi-field. Defining chained multi-fields is not supported."));
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
        return new CustomAnalyzer(null, new CharFilterFactory[0],
                new TokenFilterFactory[] { tokenFilter  });
    }

    public void testParseMeta() {
        FieldMapper.Builder<?> builder = new KeywordFieldMapper.Builder("foo");
        Mapper.TypeParser.ParserContext parserContext = new Mapper.TypeParser.ParserContext(null, null, null, null, null);

        {
            Map<String, Object> mapping = new HashMap<>(Map.of("meta", 3));
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> TypeParsers.parseField(builder, builder.name, mapping, parserContext));
            assertEquals("[meta] must be an object, got Integer[3] for field [foo]", e.getMessage());
        }

        {
            Map<String, Object> mapping = new HashMap<>(Map.of("meta", Map.of("veryloooooooooooongkey", 3L)));
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> TypeParsers.parseField(builder, builder.name, mapping, parserContext));
            assertEquals("[meta] keys can't be longer than 20 chars, but got [veryloooooooooooongkey] for field [foo]",
                    e.getMessage());
        }

        {
            Map<String, Object> mapping = new HashMap<>(Map.of("meta", Map.of(
                    "foo1", 3L, "foo2", 4L, "foo3", 5L, "foo4", 6L, "foo5", 7L, "foo6", 8L)));
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> TypeParsers.parseField(builder, builder.name, mapping, parserContext));
            assertEquals("[meta] can't have more than 5 entries, but got 6 on field [foo]",
                    e.getMessage());
        }

        {
            Map<String, Object> mapping = new HashMap<>(Map.of("meta", Map.of("foo", Map.of("bar", "baz"))));
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> TypeParsers.parseField(builder, builder.name, mapping, parserContext));
            assertEquals("[meta] values can only be strings, but got Map1[{bar=baz}] for field [foo]",
                    e.getMessage());
        }

        {
            Map<String, Object> mapping = new HashMap<>(Map.of("meta", Map.of("bar", "baz", "foo", 3)));
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> TypeParsers.parseField(builder, builder.name, mapping, parserContext));
            assertEquals("[meta] values can only be strings, but got Integer[3] for field [foo]",
                    e.getMessage());
        }

        {
            Map<String, String> meta = new HashMap<>();
            meta.put("foo", null);
            Map<String, Object> mapping = new HashMap<>(Map.of("meta", meta));
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> TypeParsers.parseField(builder, builder.name, mapping, parserContext));
            assertEquals("[meta] values can't be null (field [foo])",
                    e.getMessage());
        }

        {
            String longString = IntStream.range(0, 51)
                    .mapToObj(Integer::toString)
                    .collect(Collectors.joining());
            Map<String, Object> mapping = new HashMap<>(Map.of("meta", Map.of("foo", longString)));
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> TypeParsers.parseField(builder, builder.name, mapping, parserContext));
            assertThat(e.getMessage(), Matchers.startsWith("[meta] values can't be longer than 50 chars"));
        }
    }
}
