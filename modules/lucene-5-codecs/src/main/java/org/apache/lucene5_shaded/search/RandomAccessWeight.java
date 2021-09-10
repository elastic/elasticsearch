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
import org.apache.lucene5_shaded.util.Bits.MatchNoBits;

/**
 * Base class to build {@link Weight}s that are based on random-access
 * structures such as live docs or doc values. Such weights return a
 * {@link Scorer} which consists of an approximation that matches
 * everything, and a confirmation phase that first checks live docs and
 * then the {@link Bits} returned by {@link #getMatchingDocs(LeafReaderContext)}.
 * @lucene.internal
 */
public abstract class RandomAccessWeight extends ConstantScoreWeight {

  /** Sole constructor. */
  protected RandomAccessWeight(Query query) {
    super(query);
  }

  /**
   * Return a {@link Bits} instance representing documents that match this
   * weight on the given context. A return value of {@code null} indicates
   * that no documents matched.
   * Note: it is not needed to care about live docs as they will be checked
   * before the returned bits.
   */
  protected abstract Bits getMatchingDocs(LeafReaderContext context) throws IOException;

  @Override
  public final Scorer scorer(LeafReaderContext context) throws IOException {
    final Bits matchingDocs = getMatchingDocs(context);
    if (matchingDocs == null || matchingDocs instanceof MatchNoBits) {
      return null;
    }
    final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
    final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {

      @Override
      public boolean matches() throws IOException {
        final int doc = approximation.docID();

        return matchingDocs.get(doc);
      }

      @Override
      public float matchCost() {
        return 10; // TODO: use some cost of matchingDocs
      }
    };

    return new ConstantScoreScorer(this, score(), twoPhase);
  }

}

