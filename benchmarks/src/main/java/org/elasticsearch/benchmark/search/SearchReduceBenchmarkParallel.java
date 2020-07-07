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

package org.elasticsearch.benchmark.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchProgressListener;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class SearchReduceBenchmarkParallel {

    class TestThreadPool extends ThreadPool {

        public TestThreadPool(String name, ExecutorBuilder<?>... customBuilders) {
            this(name, Settings.EMPTY, customBuilders);
        }

        public TestThreadPool(String name, Settings settings, ExecutorBuilder<?>... customBuilders) {
            super(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), name).put(settings).build(), customBuilders);
        }

    }

    private final ThreadPool threadPool = new TestThreadPool("test");
    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
    private SearchPhaseController searchPhaseController = new SearchPhaseController(
        namedWriteableRegistry,
        s -> new InternalAggregation.ReduceContextBuilder() {
            @Override
            public InternalAggregation.ReduceContext forPartialReduction() {
                return InternalAggregation.ReduceContext.forPartialReduction(
                    BigArrays.NON_RECYCLING_INSTANCE, null, () -> PipelineAggregator.PipelineTree.EMPTY);
            }

            @Override
            public InternalAggregation.ReduceContext forFinalReduction() {
                return InternalAggregation.ReduceContext.forFinalReduction(
                    BigArrays.NON_RECYCLING_INSTANCE, null, b -> {}, PipelineAggregator.PipelineTree.EMPTY);
            }
        },
        threadPool);

    @State(Scope.Benchmark)
    public static class TermsList extends AbstractList<InternalAggregations> {

        @Param({"100", "500", "800", "1000", "2000", "5000", "10000"})
        int numShards;

        @Param({"1000"})
        int topNSize;

        @Param({"100000"})
        int cardinality;

        @Param({"1024", "512", "256", "128", "64", "32", "16", "8", "4", "2"})
        int buffSize;

        List<InternalAggregations> aggsList;

        @Setup
        public void setup() {
            this.aggsList = new ArrayList<>();
            Random rand = new Random();
            BytesRef[] dict = new BytesRef[cardinality];
            for (int i = 0; i < dict.length; i++) {
                dict[i] = new BytesRef(Long.toString(rand.nextLong()));
            }
            for (int i = 0; i < numShards; i++) {
                aggsList.add(new InternalAggregations(Collections.singletonList(newTerms(rand, dict))));
            }
        }

        private StringTerms newTerms(Random rand, BytesRef[] dict) {
            Set<BytesRef> randomTerms = new HashSet<>();
            for (int i = 0; i < topNSize; i++) {
                randomTerms.add(dict[rand.nextInt(dict.length)]);
            }
            List<StringTerms.Bucket> buckets = new ArrayList<>();
            for (BytesRef term : randomTerms) {
                buckets.add(new StringTerms.Bucket(term,
                    rand.nextInt(10000), InternalAggregations.EMPTY,
                    true, 0L, DocValueFormat.RAW));
            }
            Collections.sort(buckets, Comparator.comparingLong(a -> a.getDocCount()));
            return new StringTerms("terms", BucketOrder.key(true), topNSize, 1, Collections.emptyMap(),
                DocValueFormat.RAW, numShards, true, 0, buckets, 0);
        }

        @Override
        public InternalAggregations get(int index) {
            return aggsList.get(index);
        }

        @Override
        public int size() {
            return aggsList.size();
        }

        public int getBuffSize() {
            return buffSize;
        }
    }

    @Benchmark
    public SearchPhaseController.ReducedQueryPhase reduce(TermsList termsList) {
        List<QuerySearchResult> shards = new ArrayList<>();
        for (int i = 0; i < termsList.size(); i++) {
            QuerySearchResult result = new QuerySearchResult();
            result.setShardIndex(0);
            result.from(0);
            result.size(termsList.topNSize);
            result.aggregations(termsList.get(i));
            result.setSearchShardTarget(new SearchShardTarget("node",
                new ShardId(new Index("index", "index"), i), null, OriginalIndices.NONE));
            shards.add(result);
        }
        SearchPhaseController.QueryPhaseParallelResultConsumer consumer = new SearchPhaseController.QueryPhaseParallelResultConsumer(namedWriteableRegistry, SearchProgressListener.NOOP,
            searchPhaseController, shards.size(), false, true, 0, termsList.topNSize, new InternalAggregation.ReduceContextBuilder() {
            @Override
            public InternalAggregation.ReduceContext forPartialReduction() {
                return InternalAggregation.ReduceContext.forPartialReduction(
                    BigArrays.NON_RECYCLING_INSTANCE, null, () -> PipelineAggregator.PipelineTree.EMPTY);
            }

            @Override
            public InternalAggregation.ReduceContext forFinalReduction() {
                return InternalAggregation.ReduceContext.forFinalReduction(
                    BigArrays.NON_RECYCLING_INSTANCE, null, b -> {}, PipelineAggregator.PipelineTree.EMPTY);
            }
        }, true, threadPool);
        consumer.setParallelBuffSize(termsList.getBuffSize());
        for (int i = 0; i < shards.size(); i++) {
            consumer.consumeResult(shards.get(i));
        }
        return consumer.reduce();
    }
}
