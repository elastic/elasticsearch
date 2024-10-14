/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.rescore.RescoreContext;

import java.io.IOException;
import java.util.List;

/**
 * Explains the scoring calculations for the top hits.
 */
public final class ExplainPhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.explain() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            private final List<String> queryNames = context.queryNames();

            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                final int topLevelDocId = hitContext.hit().docId();
                Explanation explanation = context.searcher().explain(context.rewrittenQuery(), topLevelDocId);

                for (RescoreContext rescore : context.rescore()) {
                    explanation = rescore.rescorer().explain(topLevelDocId, context.searcher(), rescore, explanation);
                }

                if (context.rankBuilder() != null) {
                    // if we have nested fields, then the query is wrapped using an additional filter on the _primary_term field
                    // through the DefaultSearchContext#buildFilteredQuery so we have to extract the actual query
                    if (context.getSearchExecutionContext().nestedLookup() != NestedLookup.EMPTY) {
                        explanation = explanation.getDetails()[0];
                    }

                    if (context.rankBuilder() != null) {
                        explanation = context.rankBuilder().explainHit(explanation, hitContext.rankDoc(), queryNames);
                    }
                }
                // we use the top level doc id, since we work with the top level searcher
                hitContext.hit().explanation(explanation);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
        };
    }
}
