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

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class InnerHitsFetchSubPhase implements FetchSubPhase {

    private final FetchPhase fetchPhase;

    public InnerHitsFetchSubPhase(FetchPhase fetchPhase) {
        this.fetchPhase = fetchPhase;
    }

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {
        if ((context.innerHits() != null && context.innerHits().getInnerHits().size() > 0) == false) {
            return;
        }

        for (Map.Entry<String, InnerHitsContext.InnerHitSubContext> entry : context.innerHits().getInnerHits().entrySet()) {
            InnerHitsContext.InnerHitSubContext innerHits = entry.getValue();
            TopDocsAndMaxScore[] topDocs = innerHits.topDocs(hits);
            for (int i = 0; i < hits.length; i++) {
                SearchHit hit = hits[i];
                TopDocsAndMaxScore topDoc = topDocs[i];

                Map<String, SearchHits> results = hit.getInnerHits();
                if (results == null) {
                    hit.setInnerHits(results = new HashMap<>());
                }
                innerHits.queryResult().topDocs(topDoc, innerHits.sort() == null ? null : innerHits.sort().formats);
                int[] docIdsToLoad = new int[topDoc.topDocs.scoreDocs.length];
                for (int j = 0; j < topDoc.topDocs.scoreDocs.length; j++) {
                    docIdsToLoad[j] = topDoc.topDocs.scoreDocs[j].doc;
                }
                innerHits.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
                innerHits.setId(hit.getId());
                innerHits.lookup().source().setSource(context.lookup().source().internalSourceRef());
                if (context.lookup().source().source() != null) {
                    innerHits.lookup().source().setSource(context.lookup().source().source());
                }

                fetchPhase.execute(innerHits);
                FetchSearchResult fetchResult = innerHits.fetchResult();
                SearchHit[] internalHits = fetchResult.fetchResult().hits().getHits();
                for (int j = 0; j < internalHits.length; j++) {
                    ScoreDoc scoreDoc = topDoc.topDocs.scoreDocs[j];
                    SearchHit searchHitFields = internalHits[j];
                    searchHitFields.score(scoreDoc.score);
                    if (scoreDoc instanceof FieldDoc) {
                        FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                        searchHitFields.sortValues(fieldDoc.fields, innerHits.sort().formats);
                    }
                }
                results.put(entry.getKey(), fetchResult.hits());
            }
        }
    }
}
