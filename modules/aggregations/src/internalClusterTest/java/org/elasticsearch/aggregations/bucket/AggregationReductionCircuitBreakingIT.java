/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.aggregations.AggregationIntegTestCase;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.elasticsearch.search.aggregations.AggregationBuilders.composite;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 1, maxNumDataNodes = 2, numClientNodes = 1)
public class AggregationReductionCircuitBreakingIT extends AggregationIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // Most of the settings here exist to make the search as stable and deterministic as possible
        var settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            // More threads may lead to more consumption and the test failing in the datanodes
            .put("thread_pool.search.size", 1);
        if (NODE_ROLES_SETTING.get(otherSettings).isEmpty()) {
            // Coordinator
            settings.put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "7MB");
        } else {
            // Datanode
            // To avoid OOMs
            settings.put(USE_REAL_MEMORY_USAGE_SETTING.getKey(), true).put(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "80%");
        }
        return settings.build();
    }

    /**
     * Expect the breaker to trip in `QueryPhaseResultConsume#reduce()`, when reducing `MergeResult`s.
     * <p>
     *     After testing this, the agg serialized size is around 5MB.
     *     The CB is set to 7MB, as the reduction is expected to add an extra 50% overhead.
     * </p>
     */
    public void testCBTrippingOnReduction() throws IOException {
        createIndex();
        addDocs(100, 100, 100);

        // Some leaks (Check ESTestCase#loggedLeaks) aren't logged unless we run the test twice.
        // So we run it multiple times to ensure everything gets collected before the final test checks.
        for (int i = 0; i < 10; i++) {
            assertCBTrip(
                () -> internalCluster().coordOnlyNodeClient()
                    .prepareSearch("index")
                    .setSize(0)
                    .addAggregation(
                        composite(
                            "composite",
                            List.of(
                                new TermsValuesSourceBuilder("integer").field("integer"),
                                new TermsValuesSourceBuilder("long").field("long")
                            )
                        ).size(5000).subAggregation(topHits("top_hits").size(10))
                    )
                    .setBatchedReduceSize(randomIntBetween(2, 5)),
                e -> {
                    var completeException = ExceptionsHelper.stackTrace(e);
                    // If a shard fails, we can't check reduction
                    assumeTrue(completeException, e.shardFailures().length == 0);
                    assertThat(e.getCause(), instanceOf(CircuitBreakingException.class));
                    assertThat(completeException, containsString("QueryPhaseResultConsumer.reduce"));
                }
            );
        }
    }

    public void assertCBTrip(Supplier<SearchRequestBuilder> requestSupplier, Consumer<SearchPhaseExecutionException> exceptionCallback) {
        try {
            requestSupplier.get().get().decRef();

            fail("Expected the breaker to trip");
        } catch (SearchPhaseExecutionException e) {
            exceptionCallback.accept(e);
        }
    }

    private void createIndex() throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        mappingBuilder.startObject("properties");
        {
            mappingBuilder.startObject("integer");
            mappingBuilder.field("type", "integer");
            mappingBuilder.endObject();
        }
        {
            mappingBuilder.startObject("long");
            mappingBuilder.field("type", "long");
            mappingBuilder.endObject();
        }

        mappingBuilder.endObject(); // properties
        mappingBuilder.endObject();

        assertAcked(
            prepareCreate("index").setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 10)).build())
                .setMapping(mappingBuilder)
        );
    }

    private void addDocs(int docCount, int integerFieldMvCount, int longFieldMvCount) throws IOException {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            XContentBuilder docSource = XContentFactory.jsonBuilder();
            docSource.startObject();
            final int docNumber = i;
            List<Integer> integerValues = IntStream.range(0, integerFieldMvCount).map(x -> docNumber + x * 100).boxed().toList();
            List<Long> longValues = LongStream.range(0, longFieldMvCount).map(x -> docNumber + x * 100).boxed().toList();
            docSource.field("integer", integerValues);
            docSource.field("long", longValues);
            docSource.endObject();

            docs.add(prepareIndex("index").setOpType(DocWriteRequest.OpType.CREATE).setSource(docSource));
        }
        indexRandom(true, false, false, false, docs);
        forceMerge(false);
        flushAndRefresh("index");
    }
}
