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
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class SearchReduceBenchmarkBatched {

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
    private final Function<SearchRequest, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder = s -> new InternalAggregation.ReduceContextBuilder() {
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
    };
    private SearchPhaseController searchPhaseController = new SearchPhaseController(
        namedWriteableRegistry,
        requestToAggReduceContextBuilder,
        threadPool);

    @State(Scope.Benchmark)
    public static class TermsList extends AbstractList<InternalAggregations> {

        @Param({"100", "500", "800", "1000", "2000", "5000", "10000"})
        int numShards;

        @Param({"1000"})
        int topNSize;

        @Param({"100000"})
        int cardinality;

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
    }

    @Benchmark
    public SearchPhaseController.ReducedQueryPhase reduce(TermsList termsList) {
        List<QuerySearchResult> shards = new ArrayList<>();
        int buffSize = 512;
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

        @SuppressWarnings("unchecked")
        ArraySearchPhaseResults<SearchPhaseResult> consumer = null;

        if (shards.size() < buffSize) {
            consumer = new ArraySearchPhaseResults<SearchPhaseResult>(shards.size()) {
                @Override
                public void consumeResult(SearchPhaseResult result) {
                    super.consumeResult(result);
                }

                @Override
                public SearchPhaseController.ReducedQueryPhase reduce() {
                    List<SearchPhaseResult> resultList = results.asList();
                    final SearchPhaseController.ReducedQueryPhase reducePhase =
                        searchPhaseController.reducedQueryPhase(resultList, null, new ArrayList<>(), new SearchPhaseController.TopDocsStats(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO),
                            0, false, requestToAggReduceContextBuilder.apply(new SearchRequest()), true);
                    return reducePhase;
                }
            };
        } else {
            consumer = new SearchPhaseController.QueryPhaseResultConsumer(namedWriteableRegistry, SearchProgressListener.NOOP,
                searchPhaseController, shards.size(), buffSize, false, true, 0, termsList.topNSize, new InternalAggregation.ReduceContextBuilder() {
                @Override
                public InternalAggregation.ReduceContext forPartialReduction() {
                    return InternalAggregation.ReduceContext.forPartialReduction(
                        BigArrays.NON_RECYCLING_INSTANCE, null, () -> PipelineAggregator.PipelineTree.EMPTY);
                }

                @Override
                public InternalAggregation.ReduceContext forFinalReduction() {
                    return InternalAggregation.ReduceContext.forFinalReduction(
                        BigArrays.NON_RECYCLING_INSTANCE, null, b -> {
                        }, PipelineAggregator.PipelineTree.EMPTY);
                }
            }, true);
        }
        for (int i = 0; i < shards.size(); i++) {
            consumer.consumeResult(shards.get(i));
        }
        return consumer.reduce();
    }
}
