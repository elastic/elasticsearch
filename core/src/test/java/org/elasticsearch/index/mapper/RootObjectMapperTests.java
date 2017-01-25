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

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;

public class RootObjectMapperTests extends ESSingleNodeTestCase {

    public void testNumericDetection() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .field("numeric_detection", false)
                    .endObject()
                .endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                    .field("numeric_detection", true)
                .endObject()
            .endObject().string();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .endObject()
            .endObject().string();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateDetection() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .field("date_detection", true)
                    .endObject()
                .endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                    .field("date_detection", false)
                .endObject()
            .endObject().string();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .endObject()
            .endObject().string();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateFormatters() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .field("dynamic_date_formats", Arrays.asList("YYYY-MM-dd"))
                    .endObject()
                .endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if formatters are not set explicitly
        String mapping2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .endObject()
            .endObject().string();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                    .field("dynamic_date_formats", Arrays.asList())
                .endObject()
            .endObject().string();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testDynamicTemplates() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
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
                .endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if templates are not set explicitly
        String mapping2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .endObject()
            .endObject().string();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                    .field("dynamic_templates", Arrays.asList())
                .endObject()
            .endObject().string();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }
}
