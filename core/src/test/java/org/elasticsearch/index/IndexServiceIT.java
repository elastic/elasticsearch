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

package org.elasticsearch.index;

import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static org.elasticsearch.test.InternalSettingsPlugin.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class IndexServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(InternalSettingsPlugin.class);
    }

    public void testGlobalCheckpointSync() throws Exception {
        internalCluster().startNode();
        prepareCreate("test", Settings.builder().put(GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")).get();
        final int numberOfDocuments = randomIntBetween(1, 128);
        for (int i = 0; i < numberOfDocuments; i++) {
            final String id = Integer.toString(i);
            client().prepareIndex("test", "test", id).setSource("{\"foo\": " + id + "}", XContentType.JSON).get();
        }
        assertBusy(() -> {
            final IndicesStatsResponse stats = client().admin().indices().prepareStats().clear().get();
            final IndexStats indexStats = stats.getIndex("test");
            for (final IndexShardStats indexShardStats : indexStats.getIndexShards().values()) {
                Optional<ShardStats> maybePrimary =
                        Stream.of(indexShardStats.getShards())
                                .filter(s -> s.getShardRouting().active() && s.getShardRouting().primary())
                                .findFirst();
                if (!maybePrimary.isPresent()) {
                    continue;
                }
                final ShardStats primary = maybePrimary.get();
                final SeqNoStats primarySeqNoStats = primary.getSeqNoStats();
                for (final ShardStats shardStats : indexShardStats) {
                    final SeqNoStats seqNoStats = shardStats.getSeqNoStats();
                    assertThat(seqNoStats.getGlobalCheckpoint(), equalTo(primarySeqNoStats.getGlobalCheckpoint()));
                }
            }
        });
    }

}
