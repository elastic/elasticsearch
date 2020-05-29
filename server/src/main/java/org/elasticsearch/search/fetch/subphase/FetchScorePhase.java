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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Iterator;

public class FetchScorePhase implements FetchSubPhase {

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {
        if (context.trackScores() == false || hits.length == 0 ||
                // scores were already computed since they are needed on the coordinated node to merge top hits
                context.sort() == null) {
            return;
        }

        final IndexSearcher searcher = context.searcher();
        final Weight weight = searcher.createWeight(searcher.rewrite(context.query()), ScoreMode.COMPLETE, 1);
        Iterator<LeafReaderContext> leafContextIterator = searcher.getIndexReader().leaves().iterator();
        LeafReaderContext leafContext = null;
        Scorer scorer = null;
        for (SearchHit hit : hits) {
            if (leafContext == null || leafContext.docBase + leafContext.reader().maxDoc() <= hit.docId()) {
                do {
                    leafContext = leafContextIterator.next();
                } while (leafContext == null || leafContext.docBase + leafContext.reader().maxDoc() <= hit.docId());
                ScorerSupplier scorerSupplier = weight.scorerSupplier(leafContext);
                if (scorerSupplier == null) {
                    throw new IllegalStateException("Can't compute score on document " + hit + " as it doesn't match the query");
                }
                scorer = scorerSupplier.get(1L); // random-access
            }

            final int leafDocID = hit.docId() - leafContext.docBase;
            assert leafDocID >= 0 && leafDocID < leafContext.reader().maxDoc();
            int advanced = scorer.iterator().advance(leafDocID);
            if (advanced != leafDocID) {
                throw new IllegalStateException("Can't compute score on document " + hit + " as it doesn't match the query");
            }
            hit.score(scorer.score());
        }
    }

}
