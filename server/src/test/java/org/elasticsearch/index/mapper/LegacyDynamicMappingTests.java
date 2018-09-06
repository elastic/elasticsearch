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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class LegacyDynamicMappingTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testMixTemplateMultiFieldMultiTypeAndMappingReuse() throws Exception {
        IndexService indexService = createIndex("test", Settings.builder().put("index.version.created", Version.V_5_6_0).build());
        XContentBuilder mappings1 = jsonBuilder().startObject()
                .startObject("type1")
                .startArray("dynamic_templates")
                .startObject()
                .startObject("template1")
                .field("match_mapping_type", "string")
                .startObject("mapping")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject().endObject();
        indexService.mapperService().merge("type1", new CompressedXContent(BytesReference.bytes(mappings1)),
                MapperService.MergeReason.MAPPING_UPDATE, false);
        XContentBuilder mappings2 = jsonBuilder().startObject()
                .startObject("type2")
                .startObject("properties")
                .startObject("field")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject().endObject();
        indexService
                .mapperService()
                .merge("type2", new CompressedXContent(BytesReference.bytes(mappings2)), MapperService.MergeReason.MAPPING_UPDATE, false);

        XContentBuilder json = XContentFactory.jsonBuilder().startObject()
                .field("field", "foo")
                .endObject();
        SourceToParse source = SourceToParse.source("test", "type1", "1", BytesReference.bytes(json), json.contentType());
        DocumentMapper mapper = indexService.mapperService().documentMapper("type1");
        assertNull(mapper.mappers().getMapper("field.raw"));
        ParsedDocument parsed = mapper.parse(source);
        assertNotNull(parsed.dynamicMappingsUpdate());

        indexService
                .mapperService()
                .merge(
                        "type1",
                        new CompressedXContent(parsed.dynamicMappingsUpdate().toString()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mapper = indexService.mapperService().documentMapper("type1");
        assertNotNull(mapper.mappers().getMapper("field.raw"));
        parsed = mapper.parse(source);
        assertNull(parsed.dynamicMappingsUpdate());
    }

}
