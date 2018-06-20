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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;

public class FieldAliasMapperTests extends ESSingleNodeTestCase {
    private MapperService mapperService;
    private DocumentMapperParser parser;

    @Before
    public void setup() {
        IndexService indexService = createIndex("test");
        mapperService = indexService.mapperService();
        parser = mapperService.documentMapperParser();
    }

    public void testParsing() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("alias-field")
                            .field("type", "alias")
                            .field("path", "concrete-field")
                        .endObject()
                        .startObject("concrete-field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());
    }

    public void testParsingWithMissingPath() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("alias-field")
                            .field("type", "alias")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        MapperParsingException exception = expectThrows(MapperParsingException.class,
            () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("The [path] property must be specified for field [alias-field].", exception.getMessage());
    }

    public void testParsingWithExtraArgument() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("alias-field")
                            .field("type", "alias")
                            .field("path", "concrete-field")
                            .field("extra-field", "extra-value")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        MapperParsingException exception = expectThrows(MapperParsingException.class,
            () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Mapping definition for [alias-field] has unsupported parameters:  [extra-field : extra-value]",
            exception.getMessage());
    }

    public void testMerge() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("first-field")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("alias-field")
                            .field("type", "alias")
                            .field("path", "first-field")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        MappedFieldType firstFieldType = mapperService.fullName("alias-field");
        assertEquals("first-field", firstFieldType.name());
        assertTrue(firstFieldType instanceof KeywordFieldMapper.KeywordFieldType);

        String newMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
               .startObject("type")
                    .startObject("properties")
                        .startObject("second-field")
                            .field("type", "text")
                        .endObject()
                        .startObject("alias-field")
                            .field("type", "alias")
                            .field("path", "second-field")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        mapperService.merge("type", new CompressedXContent(newMapping), MergeReason.MAPPING_UPDATE);

        MappedFieldType secondFieldType = mapperService.fullName("alias-field");
        assertEquals("second-field", secondFieldType.name());
        assertTrue(secondFieldType instanceof TextFieldMapper.TextFieldType);
    }

    public void testMergeFailure() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("concrete-field")
                            .field("type", "text")
                        .endObject()
                        .startObject("alias-field")
                            .field("type", "alias")
                            .field("path", "concrete-field")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        String newMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
               .startObject("type")
                    .startObject("properties")
                        .startObject("alias-field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> mapperService.merge("type", new CompressedXContent(newMapping), MergeReason.MAPPING_UPDATE));
        assertEquals("Cannot merge a field alias mapping [alias-field] with a mapping that is not for a field alias.",
            exception.getMessage());
    }
}
