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

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class TransportSearchIT extends ESIntegTestCase {

    public void testShardCountLimit() throws Exception {
        try {
            final int numPrimaries1 = randomIntBetween(2, 10);
            final int numPrimaries2 = randomIntBetween(1, 10);
            assertAcked(prepareCreate("test1")
                    .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numPrimaries1)));
            assertAcked(prepareCreate("test2")
                    .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numPrimaries2)));

            // no exception
            client().prepareSearch("test1").get();

            assertAcked(client().admin().cluster().prepareUpdateSettings()
                    .setTransientSettings(Collections.singletonMap(
                            TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), numPrimaries1 - 1)));

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> client().prepareSearch("test1").get());
            assertThat(e.getMessage(), containsString("Trying to query " + numPrimaries1
                    + " shards, which is over the limit of " + (numPrimaries1 - 1)));

            assertAcked(client().admin().cluster().prepareUpdateSettings()
                    .setTransientSettings(Collections.singletonMap(
                            TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), numPrimaries1)));

            // no exception
            client().prepareSearch("test1").get();

            e = expectThrows(IllegalArgumentException.class,
                    () -> client().prepareSearch("test1", "test2").get());
            assertThat(e.getMessage(), containsString("Trying to query " + (numPrimaries1 + numPrimaries2)
                    + " shards, which is over the limit of " + numPrimaries1));

        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings()
                    .setTransientSettings(Collections.singletonMap(
                            TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), null)));
        }
    }

}
