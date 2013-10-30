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
package org.elasticsearch.index.search.slowlog;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

/**
 *
 */
public class ShardSlowLogSearchServiceTests extends ElasticsearchTestCase {

    private Index index = new Index("test");
    private ShardId shardId = new ShardId(index, 0);


    @Test
    public void creatingShardSlowLogSearchServiceWithBrokenSettingsShouldWork() throws Exception {
        Settings brokenIndexSettings = ImmutableSettings.builder()
                .put("index.search.slowlog.threshold.query.warn", "s")
                .build();

        IndexSettingsService indexSettingsService = new IndexSettingsService(shardId.index(), brokenIndexSettings);
        new ShardSlowLogSearchService(shardId, brokenIndexSettings, indexSettingsService);
    }

    @Test
    public void updatingViaListenerWithBrokenSettingsLeavesSettingsAsIs() throws Exception {
        Settings indexSettings = ImmutableSettings.builder()
                .put("index.search.slowlog.threshold.query.warn", "1s")
                .build();

        IndexSettingsService indexSettingsService = new IndexSettingsService(shardId.index(), indexSettings);
        ShardSlowLogSearchService shardSlowLogSearchService = new ShardSlowLogSearchService(shardId, indexSettings, indexSettingsService);

        Settings updatedSettings = ImmutableSettings.builder()
                .put("index.search.slowlog.threshold.query.warn", "s")
                .build();
        indexSettingsService.refreshSettings(updatedSettings);

        // this is still the time from the indexSettings above, but was not overriden from the settings update
        // this basically ensures that the parsing exception was caught in the refreshSettings() methods
        String configuredTime = shardSlowLogSearchService.indexSettings().get("index.search.slowlog.threshold.query.warn");
        assertThat(configuredTime, is("1s"));
    }

}
