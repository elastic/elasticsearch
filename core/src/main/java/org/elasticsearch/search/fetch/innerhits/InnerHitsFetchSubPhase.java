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

package org.elasticsearch.search.fetch.innerhits;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsParseElement;
import org.elasticsearch.search.fetch.script.ScriptFieldsParseElement;
import org.elasticsearch.search.fetch.source.FetchSourceParseElement;
import org.elasticsearch.search.highlight.HighlighterParseElement;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 */
public class InnerHitsFetchSubPhase implements FetchSubPhase {
    private final Map<String, ? extends SearchParseElement> parseElements;

    private FetchPhase fetchPhase;

    @Inject
    public InnerHitsFetchSubPhase(SortParseElement sortParseElement, FetchSourceParseElement sourceParseElement, HighlighterParseElement highlighterParseElement, FieldDataFieldsParseElement fieldDataFieldsParseElement, ScriptFieldsParseElement scriptFieldsParseElement) {
        parseElements = singletonMap("inner_hits", new InnerHitsParseElement(sortParseElement, sourceParseElement, highlighterParseElement,
                fieldDataFieldsParseElement, scriptFieldsParseElement));
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return parseElements;
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.innerHits() != null;
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        Map<String, InternalSearchHits> results = new HashMap<>();
        for (Map.Entry<String, InnerHitsContext.BaseInnerHits> entry : context.innerHits().getInnerHits().entrySet()) {
            InnerHitsContext.BaseInnerHits innerHits = entry.getValue();
            TopDocs topDocs;
            try {
                topDocs = innerHits.topDocs(context, hitContext);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
            innerHits.queryResult().topDocs(topDocs);
            int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
            }
            innerHits.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
            fetchPhase.execute(innerHits);
            FetchSearchResult fetchResult = innerHits.fetchResult();
            InternalSearchHit[] internalHits = fetchResult.fetchResult().hits().internalHits();
            for (int i = 0; i < internalHits.length; i++) {
                ScoreDoc scoreDoc = topDocs.scoreDocs[i];
                InternalSearchHit searchHitFields = internalHits[i];
                searchHitFields.shard(innerHits.shardTarget());
                searchHitFields.score(scoreDoc.score);
                if (scoreDoc instanceof FieldDoc) {
                    FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                    searchHitFields.sortValues(fieldDoc.fields);
                }
            }
            results.put(entry.getKey(), fetchResult.hits());
        }
        hitContext.hit().setInnerHits(results);
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) {
    }

    // To get around cyclic dependency issue
    public void setFetchPhase(FetchPhase fetchPhase) {
        this.fetchPhase = fetchPhase;
    }
}
