/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.lessThan;

public class SearchLeakIT extends ESIntegTestCase {
    // same value across all nodes for simplicity in understanding test result.
    private final String requestBreaker = randomBoolean() ? randomIntBetween(1, 100) + "mb" : null;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), true);
        if (requestBreaker != null) {
            builder.put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), requestBreaker);
        }
        return builder.build();
    }

    /**
     * Test that searches do not leak memory. The leak chased here was that global ordinals keep a reference to the reader, which could
     * hold a reference ending up pointing to the DefaultSearchContext (when low level cancellation is registered). When using heavy aggs,
     * this could cause holding on to a large piece of memory.
     */
    public void testNoLeak() throws Exception {
        logger.info("--> Using request breaker setting: {}", requestBreaker);
        // without the fix from this commit, these numbers fails the test without OOM on 1GB heap.
        // increasing to 100 and 500 makes it OOM.
        final int bulks = 50;
        final int fields = 100;
        for (int i = 0; i < bulks; ++i) {
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            final int finali = i;
            Map<String, Object> source = IntStream.range(0, fields).boxed().collect(Collectors.toMap(k -> "field" + k,
                k -> "value" + k + finali));
            for (int j = 0; j < 20; j++) {
                IndexRequestBuilder request =
                    client().prepareIndex("test").setId("id_" + i + "_" + j).setSource(source);
                bulkRequestBuilder.add(request);
            }
            assertNoFailures(bulkRequestBuilder.get());
            if ((i % 10) == 0) {
                logger.info("--> Did {} bulks", i);
            }
        }
        final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        System.gc();
        long initialUsed = memoryMXBean.getHeapMemoryUsage().getUsed();

        logger.info("--> running searches, used {}", initialUsed);
        for (int i = 0; i < fields; ++i) {
            try {
                client().prepareSearch("test")
                    .addAggregation(terms("terms").field("field" + i + ".keyword")
                        .collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()))
                        .order(BucketOrder.aggregation("cardinality", randomBoolean()))
                        .subAggregation(cardinality("cardinality").precisionThreshold(10000).field("field0.keyword")))
                    .get();
            } catch (ElasticsearchException e) {
                if (ExceptionsHelper.unwrap(e, CircuitBreakingException.class) == null) {
                    throw e;
                }
            }
            if ((i % 10) == 0) {
                logger.info("--> Did {} aggregations, used {}", i, memoryMXBean.getHeapMemoryUsage().getUsed());
            }
        }

        System.gc();
        long finalUsed = memoryMXBean.getHeapMemoryUsage().getUsed();
        logger.info("--> after test and GC, used {}", finalUsed);
        assertThat(finalUsed, lessThan(initialUsed + ByteSizeUnit.MB.toBytes(200)));
    }
}
