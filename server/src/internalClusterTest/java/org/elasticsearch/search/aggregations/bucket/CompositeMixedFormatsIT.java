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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.MissingOrder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Reproduces a suspected bug where a composite aggregation over an {@code ip} field fails to encode
 * its response when the search spans an index that maps the field as {@code ip} and an index where
 * the field is unmapped (support case: sdh-elasticsearch#10439).
 * <p>
 * {@code InternalComposite} serializes its {@code DocValueFormat}s once per composite, not per bucket,
 * and the reduced composite takes the formats of its <em>last</em> bucket. When a data node partially
 * reduces shard results (batched query phase) and the last bucket comes from the unmapped index
 * (format {@code RAW}), the coordinator reads every bucket back with {@code RAW}, and the 16-byte
 * binary {@code ip} keys are decoded as UTF-8.
 * <p>
 * The unmapped bucket is forced to be last with {@code missing_order: last}, and it only exists in the
 * unmapped index so that it cannot inherit the {@code IP} format from a mapped bucket.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeMixedFormatsIT extends ESIntegTestCase {

    private static final int SHARDS_PER_INDEX = 5;
    private static final int DOCS_PER_INDEX = 100;

    public void testCompositeOnIpWithUnmappedIndexBatched() throws Exception {
        runScenario(true);
    }

    public void testCompositeOnIpWithUnmappedIndexNotBatched() throws Exception {
        runScenario(false);
    }

    private void runScenario(boolean batched) throws Exception {
        internalCluster().startNodes(2);
        updateClusterSettings(Settings.builder().put(SearchService.BATCHED_QUERY_PHASE.getKey(), batched));
        try {
            createIndices();

            // Several rounds, as which shards are batched together on a node depends on shard allocation.
            for (int round = 0; round < 10; round++) {
                assertNoFailuresAndResponse(
                    prepareSearch("mapped", "unmapped").setSize(0)
                        .addAggregation(
                            new CompositeAggregationBuilder(
                                "composite",
                                List.of(
                                    new TermsValuesSourceBuilder("ip").field("observer.ip")
                                        .missingBucket(true)
                                        .missingOrder(MissingOrder.LAST)
                                )
                            ).size(1000)
                        ),
                    response -> {
                        CompositeAggregation composite = response.getAggregations().get("composite");
                        Set<String> ips = new TreeSet<>();
                        boolean sawMissing = false;
                        for (CompositeAggregation.Bucket bucket : composite.getBuckets()) {
                            // Reading the key formats it, which is where the production failure happens.
                            Map<String, Object> key = bucket.getKey();
                            Object ip = key.get("ip");
                            if (ip == null) {
                                sawMissing = true;
                            } else {
                                ips.add(ip.toString());
                            }
                        }
                        assertTrue("missing bucket expected", sawMissing);
                        assertThat(ips, equalTo(expectedIps()));
                    }
                );
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(SearchService.BATCHED_QUERY_PHASE.getKey()));
        }
    }

    private void createIndices() throws Exception {
        assertAcked(
            prepareCreate("mapped").setSettings(twoIndexSettings()).setMapping("observer", "type=object", "observer.ip", "type=ip")
        );
        assertAcked(prepareCreate("unmapped").setSettings(twoIndexSettings()).setMapping("other", "type=keyword"));

        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < DOCS_PER_INDEX; i++) {
            // IPv4 addresses are stored as 16-byte IPv4-mapped IPv6 (..., ff ff, a b c d): invalid UTF-8.
            docs.add(prepareIndex("mapped").setSource("observer.ip", "10.0.0." + i));
            docs.add(prepareIndex("unmapped").setSource("other", "v" + i));
        }
        indexRandom(true, docs);
    }

    private static Settings twoIndexSettings() {
        return Settings.builder().put("index.number_of_shards", SHARDS_PER_INDEX).put("index.number_of_replicas", 0).build();
    }

    private static Set<String> expectedIps() {
        Set<String> expected = new TreeSet<>();
        for (int i = 0; i < DOCS_PER_INDEX; i++) {
            expected.add("10.0.0." + i);
        }
        return expected;
    }
}
