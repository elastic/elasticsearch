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

package org.elasticsearch.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.get.GetActionIT.indexOrAlias;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class LegacyGetActionIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testGetFieldsMetaDataWithRouting() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("_doc", "field1", "type=keyword,store=true")
                .addAlias(new Alias("alias"))
                .setSettings(
                        Settings.builder()
                                .put("index.refresh_interval", -1)
                                .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_6_0_0))); // multi-types in 6.0.0

        try (XContentBuilder source = jsonBuilder().startObject().field("field1", "value").endObject()) {
            client()
                    .prepareIndex("test", "_doc", "1")
                    .setRouting("1")
                    .setSource(source)
                    .get();
        }

        {
            final GetResponse getResponse = client()
                    .prepareGet(indexOrAlias(), "_doc", "1")
                    .setRouting("1")
                    .setStoredFields("field1")
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").isMetadataField(), equalTo(false));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").isMetadataField(), equalTo(true));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
        }

        flush();

        {
            final GetResponse getResponse = client()
                    .prepareGet(indexOrAlias(), "_doc", "1")
                    .setStoredFields("field1")
                    .setRouting("1")
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").isMetadataField(), equalTo(false));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").isMetadataField(), equalTo(true));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
        }
    }

}
