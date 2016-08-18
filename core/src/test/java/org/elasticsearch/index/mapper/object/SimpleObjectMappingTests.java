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

package org.elasticsearch.index.mapper.object;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.object.ObjectMapper.Dynamic;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 */
public class SimpleObjectMappingTests extends ESSingleNodeTestCase {

    @Test
    public void testDifferentInnerObjectTokenFailure() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        try {
            defaultMapper.parse("test", "type", "1", new BytesArray(" {\n" +
                    "      \"object\": {\n" +
                    "        \"array\":[\n" +
                    "        {\n" +
                    "          \"object\": { \"value\": \"value\" }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"object\":\"value\"\n" +
                    "        }\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      \"value\":\"value\"\n" +
                    "    }"));
            fail();
        } catch (MapperParsingException e) {
            // all is well
        }
    }

    @Test
    public void testEmptyArrayProperties() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("properties").endArray()
                .endObject().endObject().string();
        createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
    }

    @Test
    public void emptyFieldsArrayMultiFieldsTest() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                                        .startObject()
                                            .startObject("tweet")
                                                .startObject("properties")
                                                    .startObject("name")
                                                        .field("type", "string")
                                                        .field("index", "analyzed")
                                                        .startArray("fields")
                                                        .endArray()
                                                    .endObject()
                                                .endObject()
                                            .endObject()
                                        .endObject()
                                        .string();
        createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
    }

    public void fieldsArrayMultiFieldsShouldThrowExceptionTest() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("tweet")
                        .startObject("properties")
                            .startObject("name")
                                .field("type", "string")
                                .field("index", "analyzed")
                                .startArray("fields")
                                    .startObject().field("test", "string").endObject()
                                    .startObject().field("test2", "string").endObject()
                                .endArray()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .string();
        try {
            createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
            fail("Expected MapperParsingException");
        } catch(MapperParsingException e) {
            assertThat(e.getMessage(), containsString("expected map for property [fields]"));
            assertThat(e.getMessage(), containsString("but got a class java.util.ArrayList"));
        }
    }

    @Test
    public void emptyFieldsArrayTest() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                                        .startObject()
                                            .startObject("tweet")
                                                .startObject("properties")
                                                    .startArray("fields")
                                                    .endArray()
                                                .endObject()
                                            .endObject()
                                        .endObject()
                                        .string();
        createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
    }

    public void fieldsWithFilledArrayShouldThrowExceptionTest() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("tweet")
                        .startObject("properties")
                            .startArray("fields")
                                .startObject().field("test", "string").endObject()
                                .startObject().field("test2", "string").endObject()
                            .endArray()
                        .endObject()
                    .endObject()
                .endObject()
                .string();
        try {
            createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
            fail("Expected MapperParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), containsString("Expected map for property [fields]"));
        }
    }

    @Test
    public void fieldPropertiesArrayTest() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                                        .startObject()
                                            .startObject("tweet")
                                                .startObject("properties")
                                                    .startObject("name")
                                                        .field("type", "string")
                                                        .field("index", "analyzed")
                                                        .startObject("fields")
                                                            .startObject("raw")
                                                                .field("type", "string")
                                                                .field("index","not_analyzed")
                                                            .endObject()
                                                        .endObject()
                                                    .endObject()
                                                .endObject()
                                            .endObject()
                                        .endObject()
                                        .string();
        createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
    }

    public void testMerge() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "string")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, false);
        assertNull(mapper.root().includeInAll());
        assertNull(mapper.root().dynamic());
        String update = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .field("include_in_all", false)
                    .field("dynamic", "strict")
                .endObject().endObject().string();
        mapper = mapperService.merge("type", new CompressedXContent(update), MergeReason.MAPPING_UPDATE, false);
        assertFalse(mapper.root().includeInAll());
        assertEquals(Dynamic.STRICT, mapper.root().dynamic());
    }
}
