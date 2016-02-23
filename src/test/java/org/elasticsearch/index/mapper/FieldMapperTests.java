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

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class FieldMapperTests extends ElasticsearchSingleNodeTest {

    public void testRandomAnalyzerOrder() {
        IndexService test = createIndex("test");
        MapperService mapperService = test.mapperService();
        DocumentMapperParser documentMapperParser = mapperService.documentMapperParser();
        Mapper.TypeParser.ParserContext parserContext = documentMapperParser.parserContext();
        StringFieldMapper.Builder builder = new StringFieldMapper.Builder("test");
        Map<String, Object> map = new HashMap<>();
        int numValues = randomIntBetween(0, 10);
        for (int i = 0; i < numValues; i++) {
            map.put("random_key_" + i, "" + i);
        }
        final boolean hasAnalyzer = randomBoolean();
        if (hasAnalyzer) {
            map.put("analyzer", "whitespace");
        }
        final boolean hasIndexAnalyzer = randomBoolean();
        if (hasIndexAnalyzer) {
            map.put("index_analyzer", "stop");
        }
        final boolean hasSearchAnalyzer = randomBoolean();
        if (hasSearchAnalyzer) {
            map.put("search_analyzer", "simple");
        }

        TypeParsers.parseField(builder, "Test", map, parserContext);
        StringFieldMapper build = builder.build(new Mapper.BuilderContext(ImmutableSettings.EMPTY, new ContentPath()));
        NamedAnalyzer index = (NamedAnalyzer) build.indexAnalyzer();
        NamedAnalyzer search = (NamedAnalyzer) build.searchAnalyzer();
        if (hasSearchAnalyzer) {
            assertEquals(search.name(), "simple");
        }
        if (hasIndexAnalyzer) {
            assertEquals(index.name(), "stop");
        }
        if (hasAnalyzer && hasIndexAnalyzer == false) {
            assertEquals(index.name(), "whitespace");
        }
        if (hasAnalyzer && hasSearchAnalyzer == false) {
            assertEquals(search.name(), "whitespace");
        }
    }

    public void testAnalyzerOrderAllSet() {
        IndexService test = createIndex("test");
        MapperService mapperService = test.mapperService();
        DocumentMapperParser documentMapperParser = mapperService.documentMapperParser();
        Mapper.TypeParser.ParserContext parserContext = documentMapperParser.parserContext();
        StringFieldMapper.Builder builder = new StringFieldMapper.Builder("test");
        Map<String, Object> map = new HashMap<>();
        map.put("analyzer", "whitespace");
        map.put("index_analyzer", "stop");
        map.put("search_analyzer", "simple");

        TypeParsers.parseField(builder, "Test", map, parserContext);
        StringFieldMapper build = builder.build(new Mapper.BuilderContext(ImmutableSettings.EMPTY, new ContentPath()));
        NamedAnalyzer index = (NamedAnalyzer) build.indexAnalyzer();
        NamedAnalyzer search = (NamedAnalyzer) build.searchAnalyzer();
        assertEquals(search.name(), "simple");
        assertEquals(index.name(), "stop");
    }

    public void testAnalyzerOrderUseDefaultForIndex() {
        IndexService test = createIndex("test");
        MapperService mapperService = test.mapperService();
        DocumentMapperParser documentMapperParser = mapperService.documentMapperParser();
        Mapper.TypeParser.ParserContext parserContext = documentMapperParser.parserContext();
        StringFieldMapper.Builder builder = new StringFieldMapper.Builder("test");
        Map<String, Object> map = new HashMap<>();
        int numValues = randomIntBetween(0, 10);
        for (int i = 0; i < numValues; i++) {
            map.put("random_key_" + i, "" + i);
        }
        map.put("analyzer", "whitespace");
        map.put("search_analyzer", "simple");

        TypeParsers.parseField(builder, "Test", map, parserContext);
        StringFieldMapper build = builder.build(new Mapper.BuilderContext(ImmutableSettings.EMPTY, new ContentPath()));
        NamedAnalyzer index = (NamedAnalyzer) build.indexAnalyzer();
        NamedAnalyzer search = (NamedAnalyzer) build.searchAnalyzer();
        assertEquals(search.name(), "simple");
        assertEquals(index.name(), "whitespace");
    }

    public void testAnalyzerOrderUseDefaultForSearch() {
        IndexService test = createIndex("test");
        MapperService mapperService = test.mapperService();
        DocumentMapperParser documentMapperParser = mapperService.documentMapperParser();
        Mapper.TypeParser.ParserContext parserContext = documentMapperParser.parserContext();
        StringFieldMapper.Builder builder = new StringFieldMapper.Builder("test");
        Map<String, Object> map = new HashMap<>();
        int numValues = randomIntBetween(0, 10);
        for (int i = 0; i < numValues; i++) {
            map.put("random_key_" + i, "" + i);
        }
        map.put("analyzer", "whitespace");
        map.put("index_analyzer", "simple");

        TypeParsers.parseField(builder, "Test", map, parserContext);
        StringFieldMapper build = builder.build(new Mapper.BuilderContext(ImmutableSettings.EMPTY, new ContentPath()));
        NamedAnalyzer index = (NamedAnalyzer) build.indexAnalyzer();
        NamedAnalyzer search = (NamedAnalyzer) build.searchAnalyzer();
        assertEquals(search.name(), "whitespace");
        assertEquals(index.name(), "simple");
    }

    public void testAnalyzerOrder() {
        IndexService test = createIndex("test");
        MapperService mapperService = test.mapperService();
        DocumentMapperParser documentMapperParser = mapperService.documentMapperParser();
        Mapper.TypeParser.ParserContext parserContext = documentMapperParser.parserContext();
        StringFieldMapper.Builder builder = new StringFieldMapper.Builder("test");
        Map<String, Object> map = new HashMap<>();
        int numValues = randomIntBetween(0, 10);
        for (int i = 0; i < numValues; i++) {
            map.put("random_key_" + i, "" + i);
        }
        map.put("analyzer", "whitespace");

        TypeParsers.parseField(builder, "Test", map, parserContext);
        StringFieldMapper build = builder.build(new Mapper.BuilderContext(ImmutableSettings.EMPTY, new ContentPath()));
        NamedAnalyzer index = (NamedAnalyzer) build.indexAnalyzer();
        NamedAnalyzer search = (NamedAnalyzer) build.searchAnalyzer();
        assertEquals(search.name(), "whitespace");
        assertEquals(index.name(), "whitespace");
    }
}
