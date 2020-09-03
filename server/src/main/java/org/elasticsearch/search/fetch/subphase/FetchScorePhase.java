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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public class FetchScorePhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(SearchContext context) throws IOException {
        if (context.trackScores() == false || context.docIdsToLoadSize() == 0 ||
            // scores were already computed since they are needed on the coordinated node to merge top hits
            context.sort() == null) {
            return null;
        }
        final IndexSearcher searcher = context.searcher();
        final Weight weight = searcher.createWeight(searcher.rewrite(context.query()), ScoreMode.COMPLETE, 1);
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
            public void process(HitContext hitContext) throws IOException {
                if (scorer == null || scorer.iterator().advance(hitContext.docId()) != hitContext.docId()) {
                    throw new IllegalStateException("Can't compute score on document " + hitContext + " as it doesn't match the query");
                }
                hitContext.hit().score(scorer.score());
            }
        };
    }
}
