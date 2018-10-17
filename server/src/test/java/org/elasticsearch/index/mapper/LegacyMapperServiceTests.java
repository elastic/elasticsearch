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
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

public class LegacyMapperServiceTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testIndexMetaDataUpdateDoesNotLoseDefaultMapper() throws IOException {
        final IndexService indexService =
                createIndex("test", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_3_0).build());
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject(MapperService.DEFAULT_MAPPING);
                {
                    builder.field("date_detection", false);
                }
                builder.endObject();
            }
            builder.endObject();
            final PutMappingRequest putMappingRequest = new PutMappingRequest();
            putMappingRequest.indices("test");
            putMappingRequest.type(MapperService.DEFAULT_MAPPING);
            putMappingRequest.source(builder);
            client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(builder).get();
        }
        assertNotNull(indexService.mapperService().documentMapper(MapperService.DEFAULT_MAPPING));
        final Settings zeroReplicasSettings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(zeroReplicasSettings).get();
        /*
         * This assertion is a guard against a previous bug that would lose the default mapper when applying a metadata update that did not
         * update the default mapping.
         */
        assertNotNull(indexService.mapperService().documentMapper(MapperService.DEFAULT_MAPPING));
    }

    public void testDefaultMappingIsDeprecatedOn6() throws IOException {
        final Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_3_0).build();
        final String mapping;
        try (XContentBuilder defaultMapping = XContentFactory.jsonBuilder()) {
            defaultMapping.startObject();
            {
                defaultMapping.startObject("_default_");
                {

                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();
            mapping = Strings.toString(defaultMapping);
        }
        final MapperService mapperService = createIndex("test", settings).mapperService();
        mapperService.merge("_default_", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        assertWarnings("[_default_] mapping is deprecated since it is not useful anymore now that indexes cannot have more than one type");
    }

}
