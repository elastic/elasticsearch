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

package org.elasticsearch.search.controller;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ShardFieldDocSortedHitQueue;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.Facets;
import org.elasticsearch.search.facets.InternalFacet;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResultProvider;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.util.collect.Iterables;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.collect.Maps;
import org.elasticsearch.util.trove.ExtTIntArrayList;
import org.elasticsearch.util.trove.ExtTObjectIntHasMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class SearchPhaseController {
    private static final ShardDoc[] EMPTY = new ShardDoc[0];

    public AggregatedDfs aggregateDfs(Iterable<DfsSearchResult> results) {
        ExtTObjectIntHasMap<Term> dfMap = new ExtTObjectIntHasMap<Term>().defaultReturnValue(-1);
        long aggMaxDoc = 0;
        for (DfsSearchResult result : results) {
            for (int i = 0; i < result.freqs().length; i++) {
                dfMap.adjustOrPutValue(result.terms()[i], result.freqs()[i], result.freqs()[i]);
            }
            aggMaxDoc += result.maxDoc();
        }
        return new AggregatedDfs(dfMap, aggMaxDoc);
    }

    public ShardDoc[] sortDocs(Collection<? extends QuerySearchResultProvider> results) {
        if (results.isEmpty()) {
            return EMPTY;
        }

        QuerySearchResultProvider queryResultProvider = Iterables.get(results, 0);

        int totalNumDocs = 0;

        int queueSize = queryResultProvider.queryResult().from() + queryResultProvider.queryResult().size();
        if (queryResultProvider.includeFetch()) {
            // if we did both query and fetch on the same go, we have fetched all the docs from each shards already, use them...
            queueSize *= results.size();
        }
        PriorityQueue queue;
        if (queryResultProvider.queryResult().topDocs() instanceof TopFieldDocs) {
            // sorting ...
            queue = new ShardFieldDocSortedHitQueue(((TopFieldDocs) queryResultProvider.queryResult().topDocs()).fields, queueSize); // we need to accumulate for all and then filter the from
            for (QuerySearchResultProvider resultProvider : results) {
                QuerySearchResult result = resultProvider.queryResult();
                ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;
                totalNumDocs += scoreDocs.length;
                for (ScoreDoc doc : scoreDocs) {
                    ShardFieldDoc nodeFieldDoc = new ShardFieldDoc(result.shardTarget(), doc.doc, doc.score, ((FieldDoc) doc).fields);
                    if (queue.insertWithOverflow(nodeFieldDoc) == nodeFieldDoc) {
                        // filled the queue, break
                        break;
                    }
                }
            }
        } else {
            queue = new ScoreDocQueue(queueSize); // we need to accumulate for all and then filter the from
            for (QuerySearchResultProvider resultProvider : results) {
                QuerySearchResult result = resultProvider.queryResult();
                ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;
                totalNumDocs += scoreDocs.length;
                for (ScoreDoc doc : scoreDocs) {
                    ShardScoreDoc nodeScoreDoc = new ShardScoreDoc(result.shardTarget(), doc.doc, doc.score);
                    if (queue.insertWithOverflow(nodeScoreDoc) == nodeScoreDoc) {
                        // filled the queue, break
                        break;
                    }
                }
            }

        }

        int resultDocsSize = queryResultProvider.queryResult().size();
        if (queryResultProvider.includeFetch()) {
            // if we did both query and fetch on the same go, we have fetched all the docs from each shards already, use them...
            resultDocsSize *= results.size();
        }
        if (totalNumDocs < queueSize) {
            resultDocsSize = totalNumDocs - queryResultProvider.queryResult().from();
        }

        if (resultDocsSize <= 0) {
            return EMPTY;
        }

        ShardDoc[] shardDocs = new ShardDoc[resultDocsSize];
        for (int i = resultDocsSize - 1; i >= 0; i--)      // put docs in array
            shardDocs[i] = (ShardDoc) queue.pop();
        return shardDocs;
    }

    public Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad(ShardDoc[] shardDocs) {
        Map<SearchShardTarget, ExtTIntArrayList> result = Maps.newHashMap();
        for (ShardDoc shardDoc : shardDocs) {
            ExtTIntArrayList list = result.get(shardDoc.shardTarget());
            if (list == null) {
                list = new ExtTIntArrayList(); // can't be shared!, uses unsafe on it later on
                result.put(shardDoc.shardTarget(), list);
            }
            list.add(shardDoc.docId());
        }
        return result;
    }

    public InternalSearchResponse merge(ShardDoc[] sortedDocs, Map<SearchShardTarget, ? extends QuerySearchResultProvider> queryResults, Map<SearchShardTarget, ? extends FetchSearchResultProvider> fetchResults) {
        // merge facets
        Facets facets = null;
        if (!queryResults.isEmpty()) {
            // we rely on the fact that the order of facets is the same on all query results
            QuerySearchResult queryResult = queryResults.values().iterator().next().queryResult();

            // we assume the facets are in the same order!
            if (queryResult.facets() != null && queryResult.facets().facets() != null && !queryResult.facets().facets().isEmpty()) {
                List<Facet> allFacets = Lists.newArrayList();
                for (QuerySearchResultProvider queryResultProvider : queryResults.values()) {
                    allFacets.addAll(queryResultProvider.queryResult().facets().facets());
                }
                List<Facet> mergedFacets = Lists.newArrayList();
                for (Facet facet : queryResult.facets().facets()) {
                    mergedFacets.add(((InternalFacet) facet).aggregate(allFacets));
                }
                facets = new Facets(mergedFacets);
            }
        }

        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        long totalHits = 0;
        for (QuerySearchResultProvider queryResultProvider : queryResults.values()) {
            totalHits += queryResultProvider.queryResult().topDocs().totalHits;
        }

        // clean the fetch counter
        for (FetchSearchResultProvider fetchSearchResultProvider : fetchResults.values()) {
            fetchSearchResultProvider.fetchResult().initCounter();
        }

        // merge hits
        List<InternalSearchHit> hits = new ArrayList<InternalSearchHit>();
        if (!fetchResults.isEmpty()) {
            for (ShardDoc shardDoc : sortedDocs) {
                FetchSearchResultProvider fetchResultProvider = fetchResults.get(shardDoc.shardTarget());
                if (fetchResultProvider == null) {
                    continue;
                }
                FetchSearchResult fetchResult = fetchResultProvider.fetchResult();
                int index = fetchResult.counterGetAndIncrement();
                if (index < fetchResult.hits().internalHits().length) {
                    InternalSearchHit searchHit = fetchResult.hits().internalHits()[index];
                    searchHit.shard(fetchResult.shardTarget());
                    hits.add(searchHit);
                }
            }
        }
        InternalSearchHits searchHits = new InternalSearchHits(hits.toArray(new InternalSearchHit[hits.size()]), totalHits);
        return new InternalSearchResponse(searchHits, facets);
    }
}
