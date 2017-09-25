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

package org.elasticsearch.index.mapper.murmur3;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;

public class Murmur3FieldMapperTests extends ESSingleNodeTestCase {

    MapperRegistry mapperRegistry;
    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        mapperRegistry = new MapperRegistry(
                Collections.singletonMap(Murmur3FieldMapper.CONTENT_TYPE, new Murmur3FieldMapper.TypeParser()),
                Collections.emptyMap());
        Supplier<QueryShardContext> queryShardContext = () -> {
            return indexService.newQueryShardContext(0, null, () -> { throw new UnsupportedOperationException(); }, null);
        };
        parser = new DocumentMapperParser(indexService.getIndexSettings(), indexService.mapperService(), indexService.getIndexAnalyzers(),
                indexService.xContentRegistry(), indexService.similarityService(), mapperRegistry, queryShardContext);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDefaults() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "murmur3")
                .endObject().endObject().endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject().bytes(),
                XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertNotNull(fields);
        assertEquals(Arrays.toString(fields), 1, fields.length);
        IndexableField field = fields[0];
        assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_NUMERIC, field.fieldType().docValuesType());
    }

    public void testDocValuesSettingNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "murmur3")
                .field("doc_values", false)
            .endObject().endObject().endObject().endObject().string();
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [doc_values] cannot be modified"));
        }

        // even setting to the default is not allowed, the setting is invalid
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "murmur3")
                .field("doc_values", true)
            .endObject().endObject().endObject().endObject().string();
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [doc_values] cannot be modified"));
        }
    }

    public void testIndexSettingNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "murmur3")
                .field("index", "not_analyzed")
            .endObject().endObject().endObject().endObject().string();
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [index] cannot be modified"));
        }

        // even setting to the default is not allowed, the setting is invalid
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "murmur3")
                .field("index", "no")
            .endObject().endObject().endObject().endObject().string();
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [index] cannot be modified"));
        }
    }

    public void testEmptyName() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("")
            .field("type", "murmur3")
            .endObject().endObject().endObject().endObject().string();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }
}
