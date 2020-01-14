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
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;

public class RootObjectMapperTests extends ESSingleNodeTestCase {

    public void testNumericDetection() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("_doc")
                        .field("numeric_detection", false)
                    .endObject()
                .endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                    .field("numeric_detection", true)
                .endObject()
            .endObject());
        mapper = mapperService.merge("_doc", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .endObject()
            .endObject());
        mapper = mapperService.merge("_doc", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateDetection() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("_doc")
                        .field("date_detection", true)
                    .endObject()
                .endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                    .field("date_detection", false)
                .endObject()
            .endObject());
        mapper = mapperService.merge("_doc", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .endObject()
            .endObject());
        mapper = mapperService.merge("_doc", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateFormatters() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("_doc")
                        .field("dynamic_date_formats", Arrays.asList("yyyy-MM-dd"))
                    .endObject()
                .endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if formatters are not set explicitly
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .endObject()
            .endObject());
        mapper = mapperService.merge("_doc", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                    .field("dynamic_date_formats", Arrays.asList())
                .endObject()
            .endObject());
        mapper = mapperService.merge("_doc", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testDynamicTemplates() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("_doc")
                        .startArray("dynamic_templates")
                            .startObject()
                                .startObject("my_template")
                                    .field("match_mapping_type", "string")
                                    .startObject("mapping")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endArray()
                    .endObject()
                .endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if templates are not set explicitly
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .endObject()
            .endObject());
        mapper = mapperService.merge("_doc", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                    .field("dynamic_templates", Arrays.asList())
                .endObject()
            .endObject());
        mapper = mapperService.merge("_doc", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testIllegalFormatField() throws Exception {
        String dynamicMapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startArray("dynamic_date_formats")
                        .startArray().value("test_format").endArray()
                    .endArray()
                .endObject()
            .endObject());
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startArray("date_formats")
                        .startArray().value("test_format").endArray()
                    .endArray()
                .endObject()
            .endObject());

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        for (String m : Arrays.asList(mapping, dynamicMapping)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> parser.parse("type", new CompressedXContent(m)));
            assertEquals("Invalid format: [[test_format]]: expected string value", e.getMessage());
        }
    }
}
