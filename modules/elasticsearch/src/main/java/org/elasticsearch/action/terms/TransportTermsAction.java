/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.terms;

import com.google.inject.Inject;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.BoundedTreeSet;
import org.elasticsearch.util.gnu.trove.TObjectIntHashMap;
import org.elasticsearch.util.gnu.trove.TObjectIntIterator;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.*;
import static org.elasticsearch.action.Actions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportTermsAction extends TransportBroadcastOperationAction<TermsRequest, TermsResponse, ShardTermsRequest, ShardTermsResponse> {

    @Inject public TransportTermsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService, indicesService);
    }

    @Override protected TermsResponse newResponse(final TermsRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        long numDocs = 0;
        long maxDoc = 0;
        long numDeletedDocs = 0;
        List<ShardOperationFailedException> shardFailures = null;
        ShardTermsResponse aggregator = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                failedShards++;
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                ShardTermsResponse shardTermsResponse = (ShardTermsResponse) shardResponse;
                if (aggregator == null) {
                    aggregator = shardTermsResponse;
                } else {
                    for (Map.Entry<String, TObjectIntHashMap<String>> entry : shardTermsResponse.fieldsTermsFreqs().entrySet()) {
                        String fieldName = entry.getKey();
                        TObjectIntHashMap<String> termsFreqs = aggregator.fieldsTermsFreqs().get(fieldName);
                        if (termsFreqs == null) {
                            termsFreqs = new TObjectIntHashMap<String>();
                            aggregator.put(fieldName, termsFreqs);
                        }
                        for (TObjectIntIterator<String> it = entry.getValue().iterator(); it.hasNext();) {
                            it.advance();
                            termsFreqs.adjustOrPutValue(it.key(), it.value(), it.value());
                        }
                    }
                }
                numDocs += shardTermsResponse.numDocs();
                maxDoc += shardTermsResponse.maxDoc();
                numDeletedDocs += shardTermsResponse.numDeletedDocs();
                successfulShards++;
            }
        }

        Map<String, NavigableSet<TermFreq>> fieldTermsFreqs = new HashMap<String, NavigableSet<TermFreq>>();
        if (aggregator != null) {
            for (Map.Entry<String, TObjectIntHashMap<String>> entry : aggregator.fieldsTermsFreqs().entrySet()) {
                String fieldName = entry.getKey();
                NavigableSet<TermFreq> sortedFreqs = fieldTermsFreqs.get(fieldName);
                if (sortedFreqs == null) {
                    Comparator<TermFreq> comparator = request.sortType() == TermsRequest.SortType.FREQ ? TermFreq.freqComparator() : TermFreq.termComparator();
                    sortedFreqs = new BoundedTreeSet<TermFreq>(comparator, request.size());
                    fieldTermsFreqs.put(fieldName, sortedFreqs);
                }
                for (TObjectIntIterator<String> it = entry.getValue().iterator(); it.hasNext();) {
                    it.advance();
                    if (it.value() >= request.minFreq() && it.value() <= request.maxFreq()) {
                        sortedFreqs.add(new TermFreq(it.key(), it.value()));
                    }
                }
            }
        }

        FieldTermsFreq[] resultFreqs = new FieldTermsFreq[fieldTermsFreqs.size()];
        int index = 0;
        for (Map.Entry<String, NavigableSet<TermFreq>> entry : fieldTermsFreqs.entrySet()) {
            TermFreq[] freqs = entry.getValue().toArray(new TermFreq[entry.getValue().size()]);
            resultFreqs[index++] = new FieldTermsFreq(entry.getKey(), freqs);
        }
        return new TermsResponse(successfulShards, failedShards, shardFailures, resultFreqs, numDocs, maxDoc, numDeletedDocs);
    }

    @Override protected ShardTermsResponse shardOperation(ShardTermsRequest request) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard shard = indexService.shard(request.shardId());
        Engine.Searcher searcher = shard.searcher();

        ShardTermsResponse response = new ShardTermsResponse(request.index(), request.shardId(),
                searcher.reader().numDocs(), searcher.reader().maxDoc(), searcher.reader().numDeletedDocs());
        TermDocs termDocs = null;
        try {
            Pattern regexpPattern = null;
            if (request.regexp() != null) {
                regexpPattern = Pattern.compile(request.regexp(), Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
            }
            for (String fieldName : request.fields()) {
                TObjectIntHashMap<String> termsFreqs = new TObjectIntHashMap<String>();

                FieldMapper fieldMapper = indexService.mapperService().smartNameFieldMapper(fieldName);
                String indexFieldName = fieldName;
                if (fieldMapper != null) {
                    indexFieldName = fieldMapper.names().indexName();
                }
                indexFieldName = StringHelper.intern(indexFieldName);

                // setup the to and from
                String from = request.from();
                if (from == null) {
                    from = request.prefix();
                } else {
                    if (request.convert()) {
                        if (fieldMapper != null) {
                            from = fieldMapper.indexedValue(from);
                        }
                    }
                }
                if (from == null) {
                    from = "";
                }
                Term fromTerm = new Term(indexFieldName, from);

                String to = request.to();
                if (to != null && request.convert() && fieldMapper != null) {
                    to = fieldMapper.indexedValue(to);
                }
                Term toTerm = to == null ? null : new Term(indexFieldName, to);

                TermEnum termEnum = null;
                try {
                    termEnum = searcher.reader().terms(fromTerm);

                    // skip the first if we are not inclusive on from
                    if (!request.fromInclusive() && request.from() != null) {
                        termEnum.next();
                    }

                    if (request.sortType() == TermsRequest.SortType.TERM) {
                        int counter = 0;
                        while (counter < request.size()) {
                            Term term = termEnum.term();
                            // have we reached the end?
                            if (term == null || indexFieldName != term.field()) { // StirngHelper.intern
                                break;
                            }
                            // does it match on the prefix?
                            if (request.prefix() != null && !term.text().startsWith(request.prefix())) {
                                break;
                            }
                            // does it match on regexp?
                            if (regexpPattern != null && !regexpPattern.matcher(term.text()).matches()) {
                                termEnum.next();
                                continue;
                            }
                            // check on the to term
                            if (toTerm != null) {
                                int toCompareResult = term.compareTo(toTerm);
                                if (toCompareResult > 0 || (toCompareResult == 0 && !request.toInclusive())) {
                                    break;
                                }
                            }

                            int docFreq = termEnum.docFreq();
                            if (request.exact()) {
                                if (termDocs == null) {
                                    termDocs = searcher.reader().termDocs();
                                }
                                termDocs.seek(termEnum);
                                docFreq = 0;
                                while (termDocs.next()) {
                                    if (!searcher.reader().isDeleted(termDocs.doc())) {
                                        docFreq++;
                                    }
                                }
                            }
                            termsFreqs.put(term.text(), docFreq);
                            if (!termEnum.next()) {
                                break;
                            }
                            counter++;
                        }
                    } else if (request.sortType() == TermsRequest.SortType.FREQ) {
                        BoundedTreeSet<TermFreq> sortedFreq = new BoundedTreeSet<TermFreq>(TermFreq.freqComparator(), request.size());
                        while (true) {
                            Term term = termEnum.term();
                            // have we reached the end?
                            if (term == null || indexFieldName != term.field()) { // StirngHelper.intern
                                break;
                            }
                            // does it match on the prefix?
                            if (request.prefix() != null && !term.text().startsWith(request.prefix())) {
                                break;
                            }
                            // does it match on regexp?
                            if (regexpPattern != null && !regexpPattern.matcher(term.text()).matches()) {
                                termEnum.next();
                                continue;
                            }
                            // check on the to term
                            if (toTerm != null) {
                                int toCompareResult = term.compareTo(toTerm);
                                if (toCompareResult > 0 || (toCompareResult == 0 && !request.toInclusive())) {
                                    break;
                                }
                            }

                            int docFreq = termEnum.docFreq();
                            if (request.exact()) {
                                if (termDocs == null) {
                                    termDocs = searcher.reader().termDocs();
                                }
                                termDocs.seek(termEnum);
                                docFreq = 0;
                                while (termDocs.next()) {
                                    if (!searcher.reader().isDeleted(termDocs.doc())) {
                                        docFreq++;
                                    }
                                }
                            }
                            sortedFreq.add(new TermFreq(term.text(), docFreq));
                            if (!termEnum.next()) {
                                break;
                            }
                        }

                        for (TermFreq termFreq : sortedFreq) {
                            termsFreqs.put(termFreq.term(), termFreq.docFreq());
                        }
                    }

                    response.put(fieldName, termsFreqs);
                } catch (Exception e) {
                    logger.debug("Failed to get term enum from term [" + fromTerm + "]", e);
                } finally {
                    if (termEnum != null) {
                        try {
                            termEnum.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }
            }
            return response;
        } finally {
            if (termDocs != null) {
                try {
                    termDocs.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            searcher.release();
        }
    }

    @Override protected String transportAction() {
        return TransportActions.TERMS;
    }

    @Override protected String transportShardAction() {
        return "indices/terms/shard";
    }

    @Override protected TermsRequest newRequest() {
        return new TermsRequest();
    }

    @Override protected ShardTermsRequest newShardRequest() {
        return new ShardTermsRequest();
    }

    @Override protected ShardTermsRequest newShardRequest(ShardRouting shard, TermsRequest request) {
        return new ShardTermsRequest(shard.index(), shard.id(), request);
    }

    @Override protected ShardTermsResponse newShardResponse() {
        return new ShardTermsResponse();
    }

    @Override protected GroupShardsIterator shards(TermsRequest request, ClusterState clusterState) {
        return indicesService.searchShards(clusterState, processIndices(clusterState, request.indices()), request.queryHint());
    }
}
