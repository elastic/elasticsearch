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
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class LegacyMapperServiceTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testTypes() throws Exception {
        IndexService indexService1 = createIndex("index1", Settings.builder().put("index.version.created", Version.V_5_6_0) // multi types
                .build());
        MapperService mapperService = indexService1.mapperService();
        assertEquals(Collections.emptySet(), mapperService.types());

        mapperService.merge("type1", new CompressedXContent("{\"type1\":{}}"), MapperService.MergeReason.MAPPING_UPDATE, false);
        assertNull(mapperService.documentMapper(MapperService.DEFAULT_MAPPING));
        assertEquals(Collections.singleton("type1"), mapperService.types());

        mapperService.merge(
                MapperService.DEFAULT_MAPPING,
                new CompressedXContent("{\"_default_\":{}}"), MapperService.MergeReason.MAPPING_UPDATE, false);
        assertNotNull(mapperService.documentMapper(MapperService.DEFAULT_MAPPING));
        assertEquals(Collections.singleton("type1"), mapperService.types());

        mapperService.merge("type2", new CompressedXContent("{\"type2\":{}}"), MapperService.MergeReason.MAPPING_UPDATE, false);
        assertNotNull(mapperService.documentMapper(MapperService.DEFAULT_MAPPING));
        assertEquals(new HashSet<>(Arrays.asList("type1", "type2")), mapperService.types());
    }

    public void testOtherDocumentMappersOnlyUpdatedWhenChangingFieldType() throws IOException {
        IndexService indexService = createIndex("test",
                Settings.builder().put("index.version.created", Version.V_5_6_0).build()); // multiple types

        CompressedXContent simpleMapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject("field")
                .field("type", "text")
                .endObject()
                .endObject().endObject()));

        indexService.mapperService().merge("type1", simpleMapping, MergeReason.MAPPING_UPDATE, true);
        DocumentMapper documentMapper = indexService.mapperService().documentMapper("type1");

        indexService.mapperService().merge("type2", simpleMapping, MergeReason.MAPPING_UPDATE, true);
        assertSame(indexService.mapperService().documentMapper("type1"), documentMapper);

        CompressedXContent normsDisabledMapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject("field")
                .field("type", "text")
                .field("norms", false)
                .endObject()
                .endObject().endObject()));

        indexService.mapperService().merge("type3", normsDisabledMapping, MergeReason.MAPPING_UPDATE, true);
        assertNotSame(indexService.mapperService().documentMapper("type1"), documentMapper);
    }

}
