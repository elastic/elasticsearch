/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ClusterScope(supportsDedicatedMasters = false, numDataNodes = 1, scope = Scope.SUITE)
public class InternalEngineMergeIT extends ESIntegTestCase {

    public void testMergesHappening() throws Exception {
        final int numOfShards = randomIntBetween(1, 5);
        // some settings to keep num segments low
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            )
        );
        long id = 0;
        final int rounds = scaledRandomIntBetween(50, 300);
        logger.info("Starting rounds [{}] ", rounds);
        for (int i = 0; i < rounds; ++i) {
            final int numDocs = scaledRandomIntBetween(100, 1000);
            BulkRequestBuilder request = client().prepareBulk();
            for (int j = 0; j < numDocs; ++j) {
                request.add(
                    Requests.indexRequest("test")
                        .id(Long.toString(id++))
                        .source(jsonBuilder().startObject().field("l", randomLong()).endObject())
                );
            }
            BulkResponse response = request.execute().actionGet();
            refresh();
            assertNoFailures(response);
            IndicesStatsResponse stats = client().admin().indices().prepareStats("test").setSegments(true).setMerge(true).get();
            logger.info(
                "index round [{}] - segments {}, total merges {}, current merge {}",
                i,
                stats.getPrimaries().getSegments().getCount(),
                stats.getPrimaries().getMerge().getTotal(),
                stats.getPrimaries().getMerge().getCurrent()
            );
        }
        final long upperNumberSegments = 2 * numOfShards * 10;

        assertBusy(() -> {
            IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).setMerge(true).get();
            logger.info(
                "numshards {}, segments {}, total merges {}, current merge {}",
                numOfShards,
                stats.getPrimaries().getSegments().getCount(),
                stats.getPrimaries().getMerge().getTotal(),
                stats.getPrimaries().getMerge().getCurrent()
            );
            long current = stats.getPrimaries().getMerge().getCurrent();
            long count = stats.getPrimaries().getSegments().getCount();
            assertThat(count, lessThan(upperNumberSegments));
            assertThat(current, equalTo(0L));
        });

        IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).setMerge(true).get();
        logger.info(
            "numshards {}, segments {}, total merges {}, current merge {}",
            numOfShards,
            stats.getPrimaries().getSegments().getCount(),
            stats.getPrimaries().getMerge().getTotal(),
            stats.getPrimaries().getMerge().getCurrent()
        );
        long count = stats.getPrimaries().getSegments().getCount();
        assertThat(count, lessThanOrEqualTo(upperNumberSegments));
    }

}
