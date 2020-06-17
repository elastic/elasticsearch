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
package org.elasticsearch.index.engine;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ClusterScope(supportsDedicatedMasters = false, numDataNodes = 1, scope = Scope.SUITE)
public class InternalEngineMergeIT extends ESIntegTestCase {

    public void testMergesHappening() throws Exception {
        final int numOfShards = randomIntBetween(1, 5);
        // some settings to keep num segments low
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));
        long id = 0;
        final int rounds = scaledRandomIntBetween(50, 300);
        logger.info("Starting rounds [{}] ", rounds);
        for (int i = 0; i < rounds; ++i) {
            final int numDocs = scaledRandomIntBetween(100, 1000);
            BulkRequestBuilder request = client().prepareBulk();
            for (int j = 0; j < numDocs; ++j) {
                request.add(Requests.indexRequest("test").id(Long.toString(id++))
                    .source(jsonBuilder().startObject().field("l", randomLong()).endObject()));
            }
            BulkResponse response = request.execute().actionGet();
            refresh();
            assertNoFailures(response);
            IndicesStatsResponse stats = client().admin().indices().prepareStats("test")
                .setSegments(true).setMerge(true).get();
            logger.info("index round [{}] - segments {}, total merges {}, current merge {}",
                i, stats.getPrimaries().getSegments().getCount(), stats.getPrimaries().getMerge().getTotal(),
                stats.getPrimaries().getMerge().getCurrent());
        }
        final long upperNumberSegments = 2 * numOfShards * 10;

        assertBusy(() -> {
            IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).setMerge(true).get();
            logger.info("numshards {}, segments {}, total merges {}, current merge {}", numOfShards,
                stats.getPrimaries().getSegments().getCount(), stats.getPrimaries().getMerge().getTotal(),
                stats.getPrimaries().getMerge().getCurrent());
            long current = stats.getPrimaries().getMerge().getCurrent();
            long count = stats.getPrimaries().getSegments().getCount();
            assertThat(count, lessThan(upperNumberSegments));
            assertThat(current, equalTo(0L));
        });

        IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).setMerge(true).get();
        logger.info("numshards {}, segments {}, total merges {}, current merge {}", numOfShards,
            stats.getPrimaries().getSegments().getCount(), stats.getPrimaries().getMerge().getTotal(),
            stats.getPrimaries().getMerge().getCurrent());
        long count = stats.getPrimaries().getSegments().getCount();
        assertThat(count, lessThanOrEqualTo(upperNumberSegments));
    }

}
