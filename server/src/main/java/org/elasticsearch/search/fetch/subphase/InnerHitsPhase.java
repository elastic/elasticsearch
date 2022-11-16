/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class InnerHitsPhase implements FetchSubPhase {

    private final FetchPhase fetchPhase;

    public InnerHitsPhase(FetchPhase fetchPhase) {
        this.fetchPhase = fetchPhase;
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext searchContext) {
        if (searchContext.innerHits() == null || searchContext.innerHits().getInnerHits().isEmpty()) {
            return null;
        }
        Map<String, InnerHitsContext.InnerHitSubContext> innerHits = searchContext.innerHits().getInnerHits();
        StoredFieldsSpec storedFieldsSpec = new StoredFieldsSpec(requiresSource(innerHits.values()), false, Set.of());
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return storedFieldsSpec;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                Source rootSource = searchContext.getRootSource(hitContext);
                hitExecute(innerHits, hit, rootSource);
            }
        };
    }

    private static boolean requiresSource(Collection<? extends SearchContext> subContexts) {
        boolean requiresSource = false;
        for (SearchContext sc : subContexts) {
            requiresSource |= sc.sourceRequested();
            requiresSource |= sc.fetchFieldsContext() != null;
            requiresSource |= sc.highlight() != null;
        }
        return requiresSource;
    }

    private void hitExecute(Map<String, InnerHitsContext.InnerHitSubContext> innerHits, SearchHit hit, Source rootSource)
        throws IOException {
        for (Map.Entry<String, InnerHitsContext.InnerHitSubContext> entry : innerHits.entrySet()) {
            InnerHitsContext.InnerHitSubContext innerHitsContext = entry.getValue();
            TopDocsAndMaxScore topDoc = innerHitsContext.topDocs(hit);

            Map<String, SearchHits> results = hit.getInnerHits();
            if (results == null) {
                hit.setInnerHits(results = new HashMap<>());
            }
            innerHitsContext.queryResult().topDocs(topDoc, innerHitsContext.sort() == null ? null : innerHitsContext.sort().formats);
            int[] docIdsToLoad = new int[topDoc.topDocs.scoreDocs.length];
            for (int j = 0; j < topDoc.topDocs.scoreDocs.length; j++) {
                docIdsToLoad[j] = topDoc.topDocs.scoreDocs[j].doc;
            }
            innerHitsContext.docIdsToLoad(docIdsToLoad);
            innerHitsContext.setRootId(hit.getId());
            innerHitsContext.setRootLookup(rootSource);

            fetchPhase.execute(innerHitsContext);
            FetchSearchResult fetchResult = innerHitsContext.fetchResult();
            SearchHit[] internalHits = fetchResult.fetchResult().hits().getHits();
            for (int j = 0; j < internalHits.length; j++) {
                ScoreDoc scoreDoc = topDoc.topDocs.scoreDocs[j];
                SearchHit searchHitFields = internalHits[j];
                searchHitFields.score(scoreDoc.score);
                if (scoreDoc instanceof FieldDoc fieldDoc) {
                    searchHitFields.sortValues(fieldDoc.fields, innerHitsContext.sort().formats);
                }
            }
            results.put(entry.getKey(), fetchResult.hits());
        }
    }
}
