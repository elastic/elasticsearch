/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.rescore.RescoreContext;

import java.io.IOException;

/**
 * Explains the scoring calculations for the top hits.
 */
public final class ExplainPhase implements FetchSubPhase {
    @Override
    public String name() {
        return "explain";
    }

    @Override
    public String description() {
        return "explain why the documents matched the top level query";
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.explain() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                final int topLevelDocId = hitContext.hit().docId();
                Explanation explanation = context.searcher().explain(context.query(), topLevelDocId);

                for (RescoreContext rescore : context.rescore()) {
                    explanation = rescore.rescorer().explain(topLevelDocId, context.searcher(), rescore, explanation);
                }
                // we use the top level doc id, since we work with the top level searcher
                hitContext.hit().explanation(explanation);
            }
        };
    }
}
