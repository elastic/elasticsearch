/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.search;


import java.io.IOException;

import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.ToStringUtils;

/**
 * A query that matches all documents.
 *
 */
public final class MatchAllDocsQuery extends Query {

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) {
    return new ConstantScoreWeight(this) {
      @Override
      public String toString() {
        return "weight(" + MatchAllDocsQuery.this + ")";
      }
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        return new ConstantScoreScorer(this, score(), DocIdSetIterator.all(context.reader().maxDoc()));
      }
      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        final float score = score();
        final int maxDoc = context.reader().maxDoc();
        return new BulkScorer() {
          @Override
          public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            max = Math.min(max, maxDoc);
            FakeScorer scorer = new FakeScorer();
            scorer.score = score;
            collector.setScorer(scorer);
            for (int doc = min; doc < max; ++doc) {
              scorer.doc = doc;
              if (acceptDocs == null || acceptDocs.get(doc)) {
                collector.collect(doc);
              }
            }
            return max == maxDoc ? DocIdSetIterator.NO_MORE_DOCS : max;
          }
          @Override
          public long cost() {
            return maxDoc;
          }
        };
      }
    };
  }

  @Override
  public String toString(String field) {
    return "*:*" + ToStringUtils.boost(getBoost());
  }
}
