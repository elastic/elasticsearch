/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.is;

public abstract class ShardSizeTestCase extends ESIntegTestCase {

    @Override
    protected int numberOfShards() {
        // we need at least 2
        return randomIntBetween(2, DEFAULT_MAX_NUM_SHARDS);
    }

    protected void createIdx(String keyFieldMapping) {
        assertAcked(prepareCreate("idx").setMapping("key", keyFieldMapping));
    }

    protected static String routing1; // routing key to shard 1
    protected static String routing2; // routing key to shard 2

    protected void indexData() throws Exception {

        /*


        ||          ||           size = 3, shard_size = 5               ||           shard_size = size = 3               ||
        ||==========||==================================================||===============================================||
        || shard 1: ||  "1" - 5 | "2" - 4 | "3" - 3 | "4" - 2 | "5" - 1 || "1" - 5 | "3" - 3 | "2" - 4                   ||
        ||----------||--------------------------------------------------||-----------------------------------------------||
        || shard 2: ||  "1" - 3 | "2" - 1 | "3" - 5 | "4" - 2 | "5" - 1 || "1" - 3 | "3" - 5 | "4" - 2                   ||
        ||----------||--------------------------------------------------||-----------------------------------------------||
        || reduced: ||  "1" - 8 | "2" - 5 | "3" - 8 | "4" - 4 | "5" - 2 ||                                               ||
        ||          ||                                                  || "1" - 8, "3" - 8, "2" - 4    <= WRONG         ||
        ||          ||  "1" - 8 | "3" - 8 | "2" - 5     <= CORRECT      ||                                               ||


        */

        List<IndexRequestBuilder> docs = new ArrayList<>();

        routing1 = routingKeyForShard("idx", 0);
        routing2 = routingKeyForShard("idx", 1);

        docs.addAll(indexDoc(routing1, "1", 5));
        docs.addAll(indexDoc(routing1, "2", 4));
        docs.addAll(indexDoc(routing1, "3", 3));
        docs.addAll(indexDoc(routing1, "4", 2));
        docs.addAll(indexDoc(routing1, "5", 1));

        // total docs in shard "1" = 15

        docs.addAll(indexDoc(routing2, "1", 3));
        docs.addAll(indexDoc(routing2, "2", 1));
        docs.addAll(indexDoc(routing2, "3", 5));
        docs.addAll(indexDoc(routing2, "4", 2));
        docs.addAll(indexDoc(routing2, "5", 1));

        // total docs in shard "2" = 12

        indexRandom(true, docs);

        assertNoFailuresAndResponse(prepareSearch("idx").setRouting(routing1).setQuery(matchAllQuery()), resp -> {
            long totalOnOne = resp.getHits().getTotalHits().value();
            assertThat(totalOnOne, is(15L));
        });
        assertNoFailuresAndResponse(prepareSearch("idx").setRouting(routing2).setQuery(matchAllQuery()), resp -> {
            assertNoFailures(resp);
            long totalOnTwo = resp.getHits().getTotalHits().value();
            assertThat(totalOnTwo, is(12L));
        });
    }

    protected List<IndexRequestBuilder> indexDoc(String shard, String key, int times) throws Exception {
        IndexRequestBuilder[] builders = new IndexRequestBuilder[times];
        for (int i = 0; i < times; i++) {
            builders[i] = prepareIndex("idx").setRouting(shard)
                .setSource(jsonBuilder().startObject().field("key", key).field("value", 1).endObject());
        }
        return Arrays.asList(builders);
    }
}
