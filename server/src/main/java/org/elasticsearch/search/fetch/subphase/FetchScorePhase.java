/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

public class FetchScorePhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) throws IOException {
        if (context.fetchScores() == false) {
            return null;
        }
        final IndexSearcher searcher = context.searcher();
        final Weight weight = searcher.createWeight(context.rewrittenQuery(), ScoreMode.COMPLETE, 1);
        return new FetchSubPhaseProcessor() {

            Scorer scorer;

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                ScorerSupplier scorerSupplier = weight.scorerSupplier(readerContext);
                if (scorerSupplier == null) {
                    throw new IllegalStateException("Can't compute score on document as it doesn't match the query");
                }
                scorer = scorerSupplier.get(1L); // random-access
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                if (scorer == null || scorer.iterator().advance(hitContext.docId()) != hitContext.docId()) {
                    throw new IllegalStateException("Can't compute score on document " + hitContext + " as it doesn't match the query");
                }
                hitContext.hit().score(scorer.score());
            }
        };
    }
}
