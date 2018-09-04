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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class LegacySourceFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testDefaultMappingAndNoMappingWithMapperService() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject());

        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_6_0).build();
        MapperService mapperService = createIndex("test", settings).mapperService();
        mapperService.merge(
                MapperService.DEFAULT_MAPPING, new CompressedXContent(defaultMapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        DocumentMapper mapper = mapperService.documentMapperWithAutoCreate("my_type").getDocumentMapper();
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(false));
    }

    public void testDefaultMappingAndWithMappingOverrideWithMapperService() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject());

        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_6_0).build();
        MapperService mapperService = createIndex("test", settings).mapperService();
        mapperService.merge(
                MapperService.DEFAULT_MAPPING, new CompressedXContent(defaultMapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("my_type")
                .startObject("_source").field("enabled", true).endObject()
                .endObject().endObject());
        mapperService.merge("my_type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        DocumentMapper mapper = mapperService.documentMapper("my_type");
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(true));
    }

}
