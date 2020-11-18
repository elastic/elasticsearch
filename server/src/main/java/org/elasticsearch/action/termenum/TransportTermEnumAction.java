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

package org.elasticsearch.action.termenum;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportTermEnumAction extends TransportBroadcastAction<
    TermEnumRequest,
    TermEnumResponse,
    ShardTermEnumRequest,
    ShardTermEnumResponse> {

    private final SearchService searchService;

    @Inject
    public TransportTermEnumAction(
        ClusterService clusterService,
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TermEnumAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            TermEnumRequest::new,
            ShardTermEnumRequest::new,
            ThreadPool.Names.SEARCH
        );
        this.searchService = searchService;
    }

    @Override
    protected ShardTermEnumRequest newShardRequest(int numShards, ShardRouting shard, TermEnumRequest request) {
        final ClusterState clusterState = clusterService.state();
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        final AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, shard.getIndexName(), indicesAndAliases);
        return new ShardTermEnumRequest(shard.shardId(), aliasFilter, request);
    }

    @Override
    protected ShardTermEnumResponse readShardResponse(StreamInput in) throws IOException {
        return new ShardTermEnumResponse(in);
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, TermEnumRequest request, String[] concreteIndices) {
        final String routing;

        // TODO filter out cold/frozen shards and those with DLS or FLS on the target field.

        // if (request.allShards()) {
        routing = null;
        // } else {
        // // Random routing to limit request to a single shard
        // routing = Integer.toString(Randomness.get().nextInt(1000));
        // }
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, routing, request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, "_local");
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, TermEnumRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, TermEnumRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected TermEnumResponse newResponse(TermEnumRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        boolean timedOut = false;
        List<DefaultShardOperationFailedException> shardFailures = null;
        Map<String, TermCount> combinedResults = new HashMap<String, TermCount>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                ShardTermEnumResponse str = (ShardTermEnumResponse) shardResponse;
                if (str.getTimedOut()) {
                    timedOut = true;
                }
                for (TermCount term : str.terms()) {
                    TermCount existingTc = combinedResults.get(term.getTerm());
                    if (existingTc == null) {
                        combinedResults.put(term.getTerm(), term);
                    } else {
                        // add counts
                        existingTc.addToDocCount(term.getDocCount());
                    }
                }
                successfulShards++;
            }
        }
        int size = Math.min(request.size(), combinedResults.size());
        List<TermCount> terms = new ArrayList<>(size);
        TermCount[] sortedCombinedResults = combinedResults.values().toArray(new TermCount[0]);
        if (request.sortByPopularity()) {
            // Sort by doc count descending
            Arrays.sort(sortedCombinedResults, new Comparator<TermCount>() {
                public int compare(TermCount t1, TermCount t2) {
                    return Integer.compare(t2.getDocCount(), t1.getDocCount());
                }
            });
        } else {
            // Sort alphabetically
            Arrays.sort(sortedCombinedResults, new Comparator<TermCount>() {
                public int compare(TermCount t1, TermCount t2) {
                    return t1.getTerm().compareTo(t2.getTerm());
                }
            });
        }

        for (TermCount term : sortedCombinedResults) {
            terms.add(term);
            if (terms.size() == size) {
                break;
            }
        }

        return new TermEnumResponse(terms, shardsResponses.length(), successfulShards, failedShards, shardFailures, timedOut);
    }

    static class TermCountPriorityQueue extends PriorityQueue<TermCount> {

        public TermCountPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(TermCount a, TermCount b) {
            return a.getDocCount() < b.getDocCount();
        }

    }

    @Override
    protected ShardTermEnumResponse shardOperation(ShardTermEnumRequest request, Task task) throws IOException {
        List<TermCount> termsList = new ArrayList<>();
        String error = null;
        ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(
            request.shardId(),
            request.nowInMillis(),
            request.filteringAliases()
        );
        SearchContext searchContext = searchService.createSearchContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        try {
            int timeout_millis = request.timeout();
            // SearchContext has a timer check facility but the granularity of the clock ticks is
            // tailored for longer-running searches and not necessarily those of short auto-complete style
            // term lookups like this API offers. We use our own timer here.
            long scheduledEnd = System.currentTimeMillis() + timeout_millis;

            IndexReader reader = searchContext.getQueryShardContext().searcher().getTopReaderContext().reader();
            Terms terms = MultiTerms.getTerms(reader, request.field());
            TermsEnum te = MultiTerms.getTerms(reader, request.field()).iterator();
            Automaton a;
            if (request.useRegexpSyntax()) {
                int matchFlag = request.caseInsensitive() ? RegExp.ASCII_CASE_INSENSITIVE : 0;
                RegExp re = new RegExp(request.pattern(), RegExp.ALL, matchFlag);
                a = re.toAutomaton();
            } else {
                a = request.caseInsensitive()
                    ? AutomatonQueries.caseInsensitivePrefix(request.pattern())
                    : Automata.makeString(request.pattern());
            }

            if (request.leadingWildcard()) {
                a = Operations.concatenate(Automata.makeAnyString(), a);
            }
            if (request.traillingWildcard()) {
                a = Operations.concatenate(a, Automata.makeAnyString());
            }
            a = MinimizationOperations.minimize(a, Integer.MAX_VALUE);

            // TODO make this a param and scale up based on num shards like we do with terms aggs?
            int shard_size = request.size();

            CompiledAutomaton automaton = new CompiledAutomaton(a);
            te = terms.intersect(automaton, null);

            // All the above prep might take a while - do a timer check now before we continue further.
            if (System.currentTimeMillis() > scheduledEnd) {
                return new ShardTermEnumResponse(request.shardId(), termsList, error, true);
            }

            int numTermsBetweenClockChecks = 100;
            int termCount = 0;
            if (request.sortByPopularity()) {
                // Collect most popular matches
                TermCountPriorityQueue pq = new TermCountPriorityQueue(shard_size);
                TermCount spare = null;
                while (te.next() != null) {
                    termCount++;
                    if (termCount > numTermsBetweenClockChecks) {
                        if (System.currentTimeMillis() > scheduledEnd) {
                            return new ShardTermEnumResponse(request.shardId(), termsList, error, true);
                        }
                        termCount = 0;
                    }
                    int df = te.docFreq();
                    if (df < request.minShardDocFreq()) {
                        continue;
                    }
                    BytesRef bytes = te.term();

                    if (spare == null) {
                        spare = new TermCount(bytes.utf8ToString(), df);
                    } else {
                        spare.setTerm(bytes.utf8ToString());
                        spare.setDocCount(df);
                    }
                    spare = pq.insertWithOverflow(spare);
                }
                while (pq.size() > 0) {
                    termsList.add(pq.pop());
                }
            } else {
                // Collect in alphabetical order
                while (te.next() != null) {
                    termCount++;
                    if (termCount > numTermsBetweenClockChecks) {
                        if (System.currentTimeMillis() > scheduledEnd) {
                            return new ShardTermEnumResponse(request.shardId(), termsList, error, true);
                        }
                        termCount = 0;
                    }
                    int df = te.docFreq();
                    if (df < request.minShardDocFreq()) {
                        continue;
                    }
                    BytesRef bytes = te.term();
                    termsList.add(new TermCount(bytes.utf8ToString(), df));
                    if (termsList.size() >= shard_size) {
                        break;
                    }
                }
            }

        } catch (AssertionError e) {
            error = e.getMessage();
        } finally {
            Releasables.close(searchContext);
        }

        return new ShardTermEnumResponse(request.shardId(), termsList, error, false);
    }

}
