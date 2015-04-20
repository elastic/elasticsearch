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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;

public class IndicesStatsTests extends ElasticsearchSingleNodeTest {

    public void testCommitStats() throws Exception {
        createIndex("test");
        ensureGreen("test");

        IndicesStatsResponse rsp = client().admin().indices().prepareStats("test").get();
        for (ShardStats shardStats : rsp.getIndex("test").getShards()) {
            final CommitStats commitStats = shardStats.getCommitStats();
            assertNotNull(commitStats);
            assertThat(commitStats.getGeneration(), greaterThan(0l));
            assertThat(commitStats.getUserData(), hasKey(Translog.TRANSLOG_ID_KEY));

        }
    }

}
