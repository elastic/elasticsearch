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

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TypeParsersTests extends ESTestCase {

    private static Map<String, NamedAnalyzer> defaultAnalyzers() {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put(DEFAULT_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        analyzers.put(DEFAULT_SEARCH_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        analyzers.put(DEFAULT_SEARCH_QUOTED_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        return analyzers;
    }

    public void testMultiFieldWithinMultiField() throws IOException {

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

        Mapper.TypeParser typeParser = KeywordFieldMapper.PARSER;

        Map<String, Object> fieldNode = XContentHelper.convertToMap(
            BytesReference.bytes(mapping), true, mapping.contentType()).v2();

        MapperService mapperService = mock(MapperService.class);
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(defaultAnalyzers(), Collections.emptyMap(), Collections.emptyMap());
        when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        Mapper.TypeParser.ParserContext olderContext = new Mapper.TypeParser.ParserContext(
            null, mapperService, type -> typeParser, Version.CURRENT, null, null, null);

        TextFieldMapper.PARSER.parse("some-field", fieldNode, olderContext);
        assertWarnings("At least one multi-field, [sub-field], " +
            "was encountered that itself contains a multi-field. Defining multi-fields within a multi-field is deprecated " +
            "and will no longer be supported in 8.0. To resolve the issue, all instances of [fields] " +
            "that occur within a [fields] block should be removed from the mappings, either by flattening the chained " +
            "[fields] blocks into a single level, or switching to [copy_to] if appropriate.");
    }

    public void testParseMeta() {
        {
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> TypeParsers.parseMeta("foo", 3));
            assertEquals("[meta] must be an object, got Integer[3] for field [foo]", e.getMessage());
        }

        {
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> TypeParsers.parseMeta("foo", Collections.singletonMap("veryloooooooooooongkey", 3L)));
            assertEquals("[meta] keys can't be longer than 20 chars, but got [veryloooooooooooongkey] for field [foo]",
                    e.getMessage());
        }

        {
            Map<String, String> meta = new HashMap<>();
            meta.put("foo1", "3");
            meta.put("foo2", "3");
            meta.put("foo3", "3");
            meta.put("foo4", "3");
            meta.put("foo5", "3");
            meta.put("foo6", "3");
            MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> TypeParsers.parseMeta("foo", meta));
            assertEquals("[meta] can't have more than 5 entries, but got 6 on field [foo]",
                    e.getMessage());
        }

        {
            Map<String, Object> mapping = Collections.singletonMap("foo", Collections.singletonMap("bar", "baz"));
            MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> TypeParsers.parseMeta("foo", mapping));
            assertEquals("[meta] values can only be strings, but got SingletonMap[{bar=baz}] for field [foo]",
                    e.getMessage());
        }

        {
            Map<String, Object> inner = new HashMap<>();
            inner.put("bar", "baz");
            inner.put("foo", 3);
            MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> TypeParsers.parseMeta("foo", inner));
            assertEquals("[meta] values can only be strings, but got Integer[3] for field [foo]",
                    e.getMessage());
        }

        {
            Map<String, String> meta = new HashMap<>();
            meta.put("foo", null);
            MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> TypeParsers.parseMeta("foo", meta));
            assertEquals("[meta] values can't be null (field [foo])",
                    e.getMessage());
        }

        {
            String longString = IntStream.range(0, 51)
                    .mapToObj(Integer::toString)
                    .collect(Collectors.joining());
            MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> TypeParsers.parseMeta("foo", Collections.singletonMap("foo", longString)));
            assertThat(e.getMessage(), Matchers.startsWith("[meta] values can't be longer than 50 chars"));
        }
    }
}
