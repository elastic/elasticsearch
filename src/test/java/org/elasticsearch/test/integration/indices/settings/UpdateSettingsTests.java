/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.indices.settings;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSettingsTests extends AbstractSharedClusterTest {

    @Test
    public void testOpenCloseUpdateSettings() throws Exception {

        client().admin().indices().prepareCreate("test").execute().actionGet();

        try {
            client().admin().indices().prepareUpdateSettings("test")
                    .setSettings(ImmutableSettings.settingsBuilder()
                            .put("index.refresh_interval", -1) // this one can change
                            .put("index.cache.filter.type", "none") // this one can't
                    )
                    .execute().actionGet();
            assert false;
        } catch (ElasticSearchIllegalArgumentException e) {
            // all is well
        }

        IndexMetaData indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.settings().get("index.refresh_interval"), nullValue());
        assertThat(indexMetaData.settings().get("index.cache.filter.type"), nullValue());

        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.refresh_interval", -1) // this one can change
                )
                .execute().actionGet();

        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.settings().get("index.refresh_interval"), equalTo("-1"));

        // now close the index, change the non dynamic setting, and see that it applies

        client().admin().indices().prepareClose("test").execute().actionGet();

        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.refresh_interval", "1s") // this one can change
                        .put("index.cache.filter.type", "none") // this one can't
                )
                .execute().actionGet();

        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.settings().get("index.refresh_interval"), equalTo("1s"));
        assertThat(indexMetaData.settings().get("index.cache.filter.type"), equalTo("none"));
    }
}