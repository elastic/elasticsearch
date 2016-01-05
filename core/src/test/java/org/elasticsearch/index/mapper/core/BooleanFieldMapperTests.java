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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;

public class BooleanFieldMapperTests extends ESSingleNodeTestCase {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void before() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    public void testDefaults() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "boolean").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", true)
                .endObject()
                .bytes());

        try (Directory dir = new RAMDirectory();
             IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(getRandom())))) {
            w.addDocuments(doc.docs());
            try (DirectoryReader reader = DirectoryReader.open(w, true)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                // boolean fields are indexed and have doc values by default
                assertEquals(new BytesRef("T"), leaf.terms("field").iterator().next());
                SortedNumericDocValues values = leaf.getSortedNumericDocValues("field");
                assertNotNull(values);
                values.setDocument(0);
                assertEquals(1, values.count());
                assertEquals(1, values.valueAt(0));
            }
        }
    }

    public void testSerialization() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "boolean").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper mapper = defaultMapper.mappers().getMapper("field");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"boolean\"}}", builder.string());

        // now change some parameters
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "boolean")
                    .field("doc_values", "false")
                    .field("null_value", true)
                .endObject().endObject()
                .endObject().endObject().string();

        defaultMapper = parser.parse("type", new CompressedXContent(mapping));
        mapper = defaultMapper.mappers().getMapper("field");
        builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"boolean\",\"doc_values\":false,\"null_value\":true}}", builder.string());
    }

    public void testMultiFields() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "boolean")
                        .startObject("fields")
                            .startObject("as_string")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = indexService.mapperService().merge("type", new CompressedXContent(mapping), true, false);
        assertEquals(mapping, mapper.mappingSource().toString());
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                    .field("field", false)
                .endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", source);
        assertNotNull(doc.rootDoc().getField("field.as_string"));
    }
}
