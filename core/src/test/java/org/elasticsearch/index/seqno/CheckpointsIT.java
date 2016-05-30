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
package org.elasticsearch.index.seqno;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

@TestLogging("index.shard:TRACE,index.seqno:TRACE")
public class CheckpointsIT extends ESIntegTestCase {

    public void testCheckpointsAdvance() throws Exception {
        prepareCreate("test").setSettings(
            "index.seq_no.checkpoint_sync_interval", "100ms", // update global point frequently
            "index.number_of_shards", "1" // simplify things so we know how many ops goes to the shards
        ).get();
        final List<IndexRequestBuilder> builders = new ArrayList<>();
        final int numDocs = scaledRandomIntBetween(0, 100);
        logger.info("--> will index [{}] docs", numDocs);
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("test", "type", "id_" + i).setSource("{}"));
        }
        indexRandom(randomBoolean(), false, builders);

        assertBusy(() -> {
            IndicesStatsResponse stats = client().admin().indices().prepareStats("test").clear().get();
            for (ShardStats shardStats : stats.getShards()) {
                if (shardStats.getSeqNoStats() == null) {
                    assertFalse("didn't get seq no stats for a primary " + shardStats.getShardRouting(),
                        shardStats.getShardRouting().primary());
                    continue;
                }
                logger.debug("seq_no stats for {}: {}", shardStats.getShardRouting(),
                    XContentHelper.toString(shardStats.getSeqNoStats(),
                        new ToXContent.MapParams(Collections.singletonMap("pretty", "false"))));
                final Matcher<Long> localCheckpointRule;
                if (shardStats.getShardRouting().primary()) {
                    localCheckpointRule = equalTo(numDocs - 1L);
                } else {
                    // nocommit:  recovery doesn't transfer local checkpoints yet (we don't persist them in lucene).
                    localCheckpointRule = anyOf(equalTo(numDocs - 1L), equalTo(SequenceNumbersService.NO_OPS_PERFORMED));
                }
                assertThat(shardStats.getShardRouting() + " local checkpoint mismatch",
                    shardStats.getSeqNoStats().getLocalCheckpoint(), localCheckpointRule);
                assertThat(shardStats.getShardRouting() + " global checkpoint mismatch",
                    shardStats.getSeqNoStats().getGlobalCheckpoint(), equalTo(numDocs - 1L));
                assertThat(shardStats.getShardRouting() + " max seq no mismatch",
                    shardStats.getSeqNoStats().getMaxSeqNo(), equalTo(numDocs - 1L));
            }
        });
    }
}
