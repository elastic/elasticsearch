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
package org.apache.lucene5_shaded.search.similarities;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene5_shaded.index.FieldInvertState;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.search.CollectionStatistics;
import org.apache.lucene5_shaded.search.Explanation;
import org.apache.lucene5_shaded.search.TermStatistics;
import org.apache.lucene5_shaded.util.BytesRef;

/**
 * Implements the CombSUM method for combining evidence from multiple
 * similarity values described in: Joseph A. Shaw, Edward A. Fox. 
 * In Text REtrieval Conference (1993), pp. 243-252
 * @lucene.experimental
 */
public class MultiSimilarity extends Similarity {
  /** the sub-similarities used to create the combined score */
  protected final Similarity sims[];
  
  /** Creates a MultiSimilarity which will sum the scores
   * of the provided <code>sims</code>. */
  public MultiSimilarity(Similarity sims[]) {
    this.sims = sims;
  }
  
  @Override
  public long computeNorm(FieldInvertState state) {
    return sims[0].computeNorm(state);
  }

  @Override
  public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
    SimWeight subStats[] = new SimWeight[sims.length];
    for (int i = 0; i < subStats.length; i++) {
      subStats[i] = sims[i].computeWeight(collectionStats, termStats);
    }
    return new MultiStats(subStats);
  }

  @Override
  public SimScorer simScorer(SimWeight stats, LeafReaderContext context) throws IOException {
    SimScorer subScorers[] = new SimScorer[sims.length];
    for (int i = 0; i < subScorers.length; i++) {
      subScorers[i] = sims[i].simScorer(((MultiStats)stats).subStats[i], context);
    }
    return new MultiSimScorer(subScorers);
  }
  
  static class MultiSimScorer extends SimScorer {
    private final SimScorer subScorers[];
    
    MultiSimScorer(SimScorer subScorers[]) {
      this.subScorers = subScorers;
    }
    
    @Override
    public float score(int doc, float freq) {
      float sum = 0.0f;
      for (SimScorer subScorer : subScorers) {
        sum += subScorer.score(doc, freq);
      }
      return sum;
    }

    @Override
    public Explanation explain(int doc, Explanation freq) {
      List<Explanation> subs = new ArrayList<>();
      for (SimScorer subScorer : subScorers) {
        subs.add(subScorer.explain(doc, freq));
      }
      return Explanation.match(score(doc, freq.getValue()), "sum of:", subs);
    }

    @Override
    public float computeSlopFactor(int distance) {
      return subScorers[0].computeSlopFactor(distance);
    }

    @Override
    public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
      return subScorers[0].computePayloadFactor(doc, start, end, payload);
    }
  }

  static class MultiStats extends SimWeight {
    final SimWeight subStats[];
    
    MultiStats(SimWeight subStats[]) {
      this.subStats = subStats;
    }
    
    @Override
    public float getValueForNormalization() {
      float sum = 0.0f;
      for (SimWeight stat : subStats) {
        sum += stat.getValueForNormalization();
      }
      return sum / subStats.length;
    }

    @Override
    public void normalize(float queryNorm, float boost) {
      for (SimWeight stat : subStats) {
        stat.normalize(queryNorm, boost);
      }
    }
  }
}
