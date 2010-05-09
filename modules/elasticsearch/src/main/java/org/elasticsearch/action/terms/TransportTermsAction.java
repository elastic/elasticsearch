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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.SortField;
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
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.BoundedTreeSet;
import org.elasticsearch.util.collect.Maps;
import org.elasticsearch.util.gnu.trove.TObjectIntHashMap;
import org.elasticsearch.util.gnu.trove.TObjectIntIterator;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Pattern;

import static org.elasticsearch.util.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
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
        Map<String, TObjectIntHashMap<Object>> aggregator = Maps.newHashMap();
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
                IndexService indexService = indicesService.indexServiceSafe(shardTermsResponse.index());

                for (Map.Entry<String, TObjectIntHashMap<Object>> entry : shardTermsResponse.fieldsTermsFreqs().entrySet()) {
                    String fieldName = entry.getKey();

                    FieldMapper fieldMapper = indexService.mapperService().smartNameFieldMapper(fieldName);


                    TObjectIntHashMap<Object> termsFreqs = aggregator.get(fieldName);
                    if (termsFreqs == null) {
                        termsFreqs = new TObjectIntHashMap<Object>();
                        aggregator.put(fieldName, termsFreqs);
                    }
                    for (TObjectIntIterator<Object> it = entry.getValue().iterator(); it.hasNext();) {
                        it.advance();
                        Object termValue = it.key();
                        int freq = it.value();
                        if (fieldMapper != null) {
                            termValue = fieldMapper.valueForSearch(termValue);
                        }
                        termsFreqs.adjustOrPutValue(termValue, freq, freq);
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
            for (Map.Entry<String, TObjectIntHashMap<Object>> entry : aggregator.entrySet()) {
                String fieldName = entry.getKey();
                NavigableSet<TermFreq> sortedFreqs = fieldTermsFreqs.get(fieldName);
                if (sortedFreqs == null) {
                    Comparator<TermFreq> comparator = request.sortType() == TermsRequest.SortType.FREQ ? TermFreq.freqComparator() : TermFreq.termComparator();
                    sortedFreqs = new BoundedTreeSet<TermFreq>(comparator, request.size());
                    fieldTermsFreqs.put(fieldName, sortedFreqs);
                }
                for (TObjectIntIterator<Object> it = entry.getValue().iterator(); it.hasNext();) {
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
        return new TermsResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, resultFreqs, numDocs, maxDoc, numDeletedDocs);
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
                FieldMapper fieldMapper = indexService.mapperService().smartNameFieldMapper(fieldName);
                String indexFieldName = fieldName;
                if (fieldMapper != null) {
                    indexFieldName = fieldMapper.names().indexName();
                }
                indexFieldName = StringHelper.intern(indexFieldName);

                // if we are sorting by term, and the field mapper sorting type is STRING, then do plain term extraction (which is faster)
                try {
                    ExecuteTermResult executeTermResult;
                    if (request.sortType() == TermsRequest.SortType.TERM && fieldMapper != null && (fieldMapper.sortType() == SortField.STRING || fieldMapper.sortType() == SortField.STRING_VAL)) {
                        executeTermResult = executeTermSortedStringTerm(request, indexFieldName, searcher, regexpPattern, fieldMapper, termDocs);
                    } else {
                        executeTermResult = executeTerms(request, indexFieldName, searcher, regexpPattern, fieldMapper, termDocs);
                    }
                    termDocs = executeTermResult.termDocs;
                    response.put(fieldName, executeTermResult.termsFreqs);
                } catch (Exception e) {
                    // currently, just log
                    logger.warn("Failed to fetch terms for field [" + fieldName + "]", e);
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

    static class ExecuteTermResult {
        public TObjectIntHashMap<Object> termsFreqs;
        public TermDocs termDocs;

        ExecuteTermResult(TObjectIntHashMap<Object> termsFreqs, TermDocs termDocs) {
            this.termsFreqs = termsFreqs;
            this.termDocs = termDocs;
        }
    }

    private ExecuteTermResult executeTerms(ShardTermsRequest request, String indexFieldName, Engine.Searcher searcher,
                                           @Nullable Pattern regexpPattern, @Nullable FieldMapper fieldMapper, @Nullable TermDocs termDocs) throws IOException {
        TObjectIntHashMap<Object> termsFreqs = new TObjectIntHashMap<Object>();
        String sFrom = request.from();
        if (sFrom == null) {
            // really, only make sense for strings
            sFrom = request.prefix();
        }
        Object from = sFrom;
        if (from != null && fieldMapper != null) {
            from = fieldMapper.valueFromString(sFrom);
        }

        String sTo = request.to();
        Object to = sTo;
        if (to != null && fieldMapper != null) {
            to = fieldMapper.valueFromString(sTo);
        }

        TermEnum termEnum = null;
        Comparator<TermFreq> comparator = request.sortType() == TermsRequest.SortType.TERM ? TermFreq.termComparator() : TermFreq.freqComparator();
        BoundedTreeSet<TermFreq> sortedFreq = new BoundedTreeSet<TermFreq>(comparator, request.size());
        try {
            termEnum = searcher.reader().terms(new Term(indexFieldName, ""));
            while (true) {
                Term term = termEnum.term();
                // have we reached the end?
                if (term == null || indexFieldName != term.field()) { // StirngHelper.intern
                    break;
                }
                Object termValue = term.text();
                if (fieldMapper != null) {
                    termValue = fieldMapper.valueFromTerm(term.text());
                    if (fieldMapper.shouldBreakTermEnumeration(termValue)) {
                        break;
                    }
                    if (termValue == null) {
                        continue;
                    }
                }
                // check on the from term
                if (from != null) {
                    int fromCompareResult = ((Comparable) termValue).compareTo(from);
                    if (fromCompareResult < 0 || (fromCompareResult == 0 && !request.fromInclusive())) {
                        termEnum.next();
                        continue;
                    }
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
                if (to != null) {
                    int toCompareResult = ((Comparable) termValue).compareTo(to);
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
                sortedFreq.add(new TermFreq(termValue, docFreq));
                if (!termEnum.next()) {
                    break;
                }
            }
        } finally {
            if (termEnum != null) {
                try {
                    termEnum.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        for (TermFreq termFreq : sortedFreq) {
            termsFreqs.put(termFreq.term(), termFreq.docFreq());
        }
        return new ExecuteTermResult(termsFreqs, termDocs);
    }

    private ExecuteTermResult executeTermSortedStringTerm(ShardTermsRequest request, String indexFieldName, Engine.Searcher searcher,
                                                          @Nullable Pattern regexpPattern, @Nullable FieldMapper fieldMapper, @Nullable TermDocs termDocs) throws IOException {
        TObjectIntHashMap<Object> termsFreqs = new TObjectIntHashMap<Object>();
        String from = request.from();
        if (from == null) {
            from = request.prefix();
        }
        if (from == null) {
            from = "";
        }
        Term fromTerm = new Term(indexFieldName, from);

        String to = request.to();
        if (to != null && fieldMapper != null) {
            to = fieldMapper.indexedValue(to);
        }
        Term toTerm = to == null ? null : new Term(indexFieldName, to);

        TermEnum termEnum = null;
        try {
            termEnum = searcher.reader().terms(fromTerm);

            // skip the first if we are not inclusive on from
            if (!request.fromInclusive() && request.from() != null) {
                Term term = termEnum.term();
                if (term != null && indexFieldName == term.field() && term.text().equals(request.from())) {
                    termEnum.next();
                }
            }

            if (request.sortType() == TermsRequest.SortType.TERM) {
                int counter = 0;
                while (counter < request.size()) {
                    Term term = termEnum.term();
                    // have we reached the end?
                    if (term == null || indexFieldName != term.field()) { // StirngHelper.intern
                        break;
                    }
                    // convert to actual term text
                    if (fieldMapper != null) {
                        // valueAsString returns null indicating that this is not interesting
                        Object termObj = fieldMapper.valueFromTerm(term.text());
                        // if we need to break on this term enumeration, bail
                        if (fieldMapper.shouldBreakTermEnumeration(termObj)) {
                            break;
                        }
                        if (termObj == null) {
                            termEnum.next();
                            continue;
                        }
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
            }
        } finally {
            if (termEnum != null) {
                try {
                    termEnum.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        return new ExecuteTermResult(termsFreqs, termDocs);
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
        return indicesService.searchShards(clusterState, request.indices(), request.queryHint());
    }
}
