/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark.search.aggregations;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.QueryPhaseResultConsumer;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchProgressListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class TermsReduceBenchmark {
    private final SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
    private final SearchPhaseController controller = new SearchPhaseController(
        namedWriteableRegistry,
        req -> new InternalAggregation.ReduceContextBuilder() {
            @Override
            public InternalAggregation.ReduceContext forPartialReduction() {
                return InternalAggregation.ReduceContext.forPartialReduction(null, null, () -> PipelineAggregator.PipelineTree.EMPTY);
            }

            @Override
            public InternalAggregation.ReduceContext forFinalReduction() {
                final MultiBucketConsumerService.MultiBucketConsumer bucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
                    Integer.MAX_VALUE,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                );
                return InternalAggregation.ReduceContext.forFinalReduction(
                    null,
                    null,
                    bucketConsumer,
                    PipelineAggregator.PipelineTree.EMPTY
                );
            }
        }
    );

    @State(Scope.Benchmark)
    public static class TermsList extends AbstractList<InternalAggregations> {
        @Param({ "1600172297" })
        long seed;

        @Param({ "64", "128", "512" })
        int numShards;

        @Param({ "100" })
        int topNSize;

        @Param({ "1", "10", "100" })
        int cardinalityFactor;

        List<InternalAggregations> aggsList;

        @Setup
        public void setup() {
            this.aggsList = new ArrayList<>();
            Random rand = new Random(seed);
            int cardinality = cardinalityFactor * topNSize;
            BytesRef[] dict = new BytesRef[cardinality];
            for (int i = 0; i < dict.length; i++) {
                dict[i] = new BytesRef(Long.toString(rand.nextLong()));
            }
            for (int i = 0; i < numShards; i++) {
                aggsList.add(InternalAggregations.from(Collections.singletonList(newTerms(rand, dict, true))));
            }
        }

        private StringTerms newTerms(Random rand, BytesRef[] dict, boolean withNested) {
            Set<BytesRef> randomTerms = new HashSet<>();
            for (int i = 0; i < topNSize; i++) {
                randomTerms.add(dict[rand.nextInt(dict.length)]);
            }
            List<StringTerms.Bucket> buckets = new ArrayList<>();
            for (BytesRef term : randomTerms) {
                InternalAggregations subAggs;
                if (withNested) {
                    subAggs = InternalAggregations.from(Collections.singletonList(newTerms(rand, dict, false)));
                } else {
                    subAggs = InternalAggregations.EMPTY;
                }
                buckets.add(new StringTerms.Bucket(term, rand.nextInt(10000), subAggs, true, 0L, DocValueFormat.RAW));
            }

            Collections.sort(buckets, (a, b) -> a.compareKey(b));
            return new StringTerms(
                "terms",
                BucketOrder.key(true),
                BucketOrder.count(false),
                topNSize,
                1,
                Collections.emptyMap(),
                DocValueFormat.RAW,
                numShards,
                true,
                0,
                buckets,
                null
            );
        }

        @Override
        public InternalAggregations get(int index) {
            return aggsList.get(index);
        }

        @Override
        public int size() {
            return aggsList.size();
        }
    }

    @Param({ "32", "512" })
    private int bufferSize;

    @Benchmark
    public SearchPhaseController.ReducedQueryPhase reduceAggs(TermsList candidateList) throws Exception {
        List<QuerySearchResult> shards = new ArrayList<>();
        for (int i = 0; i < candidateList.size(); i++) {
            QuerySearchResult result = new QuerySearchResult();
            result.setShardIndex(i);
            result.from(0);
            result.size(0);
            result.topDocs(
                new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(1000, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]),
                    Float.NaN
                ),
                new DocValueFormat[] { DocValueFormat.RAW }
            );
            result.aggregations(candidateList.get(i));
            result.setSearchShardTarget(
                new SearchShardTarget("node", new ShardId(new Index("index", "index"), i), null, OriginalIndices.NONE)
            );
            shards.add(result);
        }
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.terms("test")));
        request.setBatchedReduceSize(bufferSize);
        ExecutorService executor = Executors.newFixedThreadPool(1);
        QueryPhaseResultConsumer consumer = new QueryPhaseResultConsumer(
            request,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            SearchProgressListener.NOOP,
            namedWriteableRegistry,
            shards.size(),
            exc -> {}
        );
        CountDownLatch latch = new CountDownLatch(shards.size());
        for (int i = 0; i < shards.size(); i++) {
            consumer.consumeResult(shards.get(i), () -> latch.countDown());
        }
        latch.await();
        SearchPhaseController.ReducedQueryPhase phase = consumer.reduce();
        executor.shutdownNow();
        return phase;
    }
}
