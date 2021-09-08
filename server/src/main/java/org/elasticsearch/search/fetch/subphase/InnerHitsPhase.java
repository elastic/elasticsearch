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
import org.elasticsearch.search.fetch.FetchProfiler;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.profile.ProfileResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class InnerHitsPhase implements FetchSubPhase {

    private final FetchPhase fetchPhase;

    public InnerHitsPhase(FetchPhase fetchPhase) {
        this.fetchPhase = fetchPhase;
    }

    @Override
    public String name() {
        return "inner_hits";
    }

    @Override
    public String description() {
        return "fetches matching nested or parent or child documents";
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext searchContext) {
        if (searchContext.innerHits() == null || searchContext.innerHits().getInnerHits().isEmpty()) {
            return null;
        }
        Map<String, InnerHitsContext.InnerHitSubContext> innerHits = searchContext.innerHits().getInnerHits();
        return new FetchSubPhaseProcessor() {
            private List<ProfileResult> innerFetchProfiles;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                SourceLookup rootLookup = searchContext.getRootSourceLookup(hitContext);
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
                    innerHitsContext.docIdsToLoad(docIdsToLoad, docIdsToLoad.length);
                    innerHitsContext.setRootId(hit.getId());
                    innerHitsContext.setRootLookup(rootLookup);

                    FetchProfiler profiler = innerHitsContext.getProfilers() == null ? null : new FetchProfiler();
                    fetchPhase.execute(innerHitsContext, profiler);
                    FetchSearchResult fetchResult = innerHitsContext.fetchResult();
                    SearchHit[] internalHits = fetchResult.fetchResult().hits().getHits();
                    for (int j = 0; j < internalHits.length; j++) {
                        ScoreDoc scoreDoc = topDoc.topDocs.scoreDocs[j];
                        SearchHit searchHitFields = internalHits[j];
                        searchHitFields.score(scoreDoc.score);
                        if (scoreDoc instanceof FieldDoc) {
                            FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                            searchHitFields.sortValues(fieldDoc.fields, innerHitsContext.sort().formats);
                        }
                    }
                    results.put(entry.getKey(), fetchResult.hits());
                    if (profiler != null) {
                        if (innerFetchProfiles == null) {
                            innerFetchProfiles = new ArrayList<>();
                        }
                        ProfileResult profile = fetchResult.profileResult();
                        innerFetchProfiles.add(
                            new ProfileResult(
                                entry.getKey(),
                                "inner_hits",
                                profile.getTimeBreakdown(),
                                profile.getDebugInfo(),
                                profile.getTime(),
                                profile.getProfiledChildren()
                            )
                        );
                    }
                }
            }

            @Override
            public List<ProfileResult> childProfiles() {
                return innerFetchProfiles;
            }
        };
    }
}
