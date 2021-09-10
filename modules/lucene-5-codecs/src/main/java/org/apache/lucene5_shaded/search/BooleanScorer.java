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
import java.util.Arrays;
import java.util.Collection;

import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.PriorityQueue;

/**
 * {@link BulkScorer} that is used for pure disjunctions and disjunctions
 * that have low values of {@link BooleanQuery.Builder#setMinimumNumberShouldMatch(int)}
 * and dense clauses. This scorer scores documents by batches of 2048 docs.
 */
final class BooleanScorer extends BulkScorer {

  static final int SHIFT = 11;
  static final int SIZE = 1 << SHIFT;
  static final int MASK = SIZE - 1;
  static final int SET_SIZE = 1 << (SHIFT - 6);
  static final int SET_MASK = SET_SIZE - 1;

  static class Bucket {
    double score;
    int freq;
  }

  private class BulkScorerAndDoc {
    final BulkScorer scorer;
    final long cost;
    int next;

    BulkScorerAndDoc(BulkScorer scorer) {
      this.scorer = scorer;
      this.cost = scorer.cost();
      this.next = -1;
    }

    void advance(int min) throws IOException {
      score(orCollector, null, min, min);
    }

    void score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
      next = scorer.score(collector, acceptDocs, min, max);
    }
  }

  // See MinShouldMatchSumScorer for an explanation
  private static long cost(Collection<BulkScorer> scorers, int minShouldMatch) {
    final PriorityQueue<BulkScorer> pq = new PriorityQueue<BulkScorer>(scorers.size() - minShouldMatch + 1) {
      @Override
      protected boolean lessThan(BulkScorer a, BulkScorer b) {
        return a.cost() > b.cost();
      }
    };
    for (BulkScorer scorer : scorers) {
      pq.insertWithOverflow(scorer);
    }
    long cost = 0;
    for (BulkScorer scorer = pq.pop(); scorer != null; scorer = pq.pop()) {
      cost += scorer.cost();
    }
    return cost;
  }

  static final class HeadPriorityQueue extends PriorityQueue<BulkScorerAndDoc> {

    public HeadPriorityQueue(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(BulkScorerAndDoc a, BulkScorerAndDoc b) {
      return a.next < b.next;
    }

  }

  static final class TailPriorityQueue extends PriorityQueue<BulkScorerAndDoc> {

    public TailPriorityQueue(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(BulkScorerAndDoc a, BulkScorerAndDoc b) {
      return a.cost < b.cost;
    }

    public BulkScorerAndDoc get(int i) {
      if (i < 0 || i >= size()) {
        throw new IndexOutOfBoundsException();
      }
      return (BulkScorerAndDoc) getHeapArray()[1 + i];
    }

  }

  final Bucket[] buckets = new Bucket[SIZE];
  // This is basically an inlined FixedBitSet... seems to help with bound checks
  final long[] matching = new long[SET_SIZE];

  final float[] coordFactors;
  final BulkScorerAndDoc[] leads;
  final HeadPriorityQueue head;
  final TailPriorityQueue tail;
  final FakeScorer fakeScorer = new FakeScorer();
  final int minShouldMatch;
  final long cost;

  final class OrCollector implements LeafCollector {
    Scorer scorer;

    @Override
    public void setScorer(Scorer scorer) {
      this.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
      final int i = doc & MASK;
      final int idx = i >>> 6;
      matching[idx] |= 1L << i;
      final Bucket bucket = buckets[i];
      bucket.freq++;
      bucket.score += scorer.score();
    }
  }

  final OrCollector orCollector = new OrCollector();

  BooleanScorer(BooleanWeight weight, boolean disableCoord, int maxCoord, Collection<BulkScorer> scorers, int minShouldMatch, boolean needsScores) {
    if (minShouldMatch < 1 || minShouldMatch > scorers.size()) {
      throw new IllegalArgumentException("minShouldMatch should be within 1..num_scorers. Got " + minShouldMatch);
    }
    if (scorers.size() <= 1) {
      throw new IllegalArgumentException("This scorer can only be used with two scorers or more, got " + scorers.size());
    }
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = new Bucket();
    }
    this.leads = new BulkScorerAndDoc[scorers.size()];
    this.head = new HeadPriorityQueue(scorers.size() - minShouldMatch + 1);
    this.tail = new TailPriorityQueue(minShouldMatch - 1);
    this.minShouldMatch = minShouldMatch;
    for (BulkScorer scorer : scorers) {
      if (needsScores == false) {
        // OrCollector calls score() all the time so we have to explicitly
        // disable scoring in order to avoid decoding useless norms
        scorer = BooleanWeight.disableScoring(scorer);
      }
      final BulkScorerAndDoc evicted = tail.insertWithOverflow(new BulkScorerAndDoc(scorer));
      if (evicted != null) {
        head.add(evicted);
      }
    }
    this.cost = cost(scorers, minShouldMatch);

    coordFactors = new float[scorers.size() + 1];
    for (int i = 0; i < coordFactors.length; i++) {
      coordFactors[i] = disableCoord ? 1.0f : weight.coord(i, maxCoord);
    }
  }

  @Override
  public long cost() {
    return cost;
  }

  private void scoreDocument(LeafCollector collector, int base, int i) throws IOException {
    final FakeScorer fakeScorer = this.fakeScorer;
    final Bucket bucket = buckets[i];
    if (bucket.freq >= minShouldMatch) {
      fakeScorer.freq = bucket.freq;
      fakeScorer.score = (float) bucket.score * coordFactors[bucket.freq];
      final int doc = base | i;
      fakeScorer.doc = doc;
      collector.collect(doc);
    }
    bucket.freq = 0;
    bucket.score = 0;
  }

  private void scoreMatches(LeafCollector collector, int base) throws IOException {
    long matching[] = this.matching;
    for (int idx = 0; idx < matching.length; idx++) {
      long bits = matching[idx];
      while (bits != 0L) {
        int ntz = Long.numberOfTrailingZeros(bits);
        int doc = idx << 6 | ntz;
        scoreDocument(collector, base, doc);
        bits ^= 1L << ntz;
      }
    }
  }

  private void scoreWindowIntoBitSetAndReplay(LeafCollector collector, Bits acceptDocs,
      int base, int min, int max, BulkScorerAndDoc[] scorers, int numScorers) throws IOException {
    for (int i = 0; i < numScorers; ++i) {
      final BulkScorerAndDoc scorer = scorers[i];
      assert scorer.next < max;
      scorer.score(orCollector, acceptDocs, min, max);
    }

    scoreMatches(collector, base);
    Arrays.fill(matching, 0L);
  }

  private BulkScorerAndDoc advance(int min) throws IOException {
    assert tail.size() == minShouldMatch - 1;
    final HeadPriorityQueue head = this.head;
    final TailPriorityQueue tail = this.tail;
    BulkScorerAndDoc headTop = head.top();
    BulkScorerAndDoc tailTop = tail.top();
    while (headTop.next < min) {
      if (tailTop == null || headTop.cost <= tailTop.cost) {
        headTop.advance(min);
        headTop = head.updateTop();
      } else {
        // swap the top of head and tail
        final BulkScorerAndDoc previousHeadTop = headTop;
        tailTop.advance(min);
        headTop = head.updateTop(tailTop);
        tailTop = tail.updateTop(previousHeadTop);
      }
    }
    return headTop;
  }

  private void scoreWindowMultipleScorers(LeafCollector collector, Bits acceptDocs, int windowBase, int windowMin, int windowMax, int maxFreq) throws IOException {
    while (maxFreq < minShouldMatch && maxFreq + tail.size() >= minShouldMatch) {
      // a match is still possible
      final BulkScorerAndDoc candidate = tail.pop();
      candidate.advance(windowMin);
      if (candidate.next < windowMax) {
        leads[maxFreq++] = candidate;
      } else {
        head.add(candidate);
      }
    }

    if (maxFreq >= minShouldMatch) {
      // There might be matches in other scorers from the tail too
      for (int i = 0; i < tail.size(); ++i) {
        leads[maxFreq++] = tail.get(i);
      }
      tail.clear();

      scoreWindowIntoBitSetAndReplay(collector, acceptDocs, windowBase, windowMin, windowMax, leads, maxFreq);
    }

    // Push back scorers into head and tail
    for (int i = 0; i < maxFreq; ++i) {
      final BulkScorerAndDoc evicted = head.insertWithOverflow(leads[i]);
      if (evicted != null) {
        tail.add(evicted);
      }
    }
  }

  private void scoreWindowSingleScorer(BulkScorerAndDoc bulkScorer, LeafCollector collector, LeafCollector singleClauseCollector,
      Bits acceptDocs, int windowMin, int windowMax, int max) throws IOException {
    assert tail.size() == 0;
    final int nextWindowBase = head.top().next & ~MASK;
    final int end = Math.max(windowMax, Math.min(max, nextWindowBase));
    
    bulkScorer.score(singleClauseCollector, acceptDocs, windowMin, end);
    
    // reset the scorer that should be used for the general case
    collector.setScorer(fakeScorer);
  }

  private BulkScorerAndDoc scoreWindow(BulkScorerAndDoc top, LeafCollector collector,
      LeafCollector singleClauseCollector, Bits acceptDocs, int min, int max) throws IOException {
    final int windowBase = top.next & ~MASK; // find the window that the next match belongs to
    final int windowMin = Math.max(min, windowBase);
    final int windowMax = Math.min(max, windowBase + SIZE);

    // Fill 'leads' with all scorers from 'head' that are in the right window
    leads[0] = head.pop();
    int maxFreq = 1;
    while (head.size() > 0 && head.top().next < windowMax) {
      leads[maxFreq++] = head.pop();
    }

    if (minShouldMatch == 1 && maxFreq == 1) {
      // special case: only one scorer can match in the current window,
      // we can collect directly
      final BulkScorerAndDoc bulkScorer = leads[0];
      scoreWindowSingleScorer(bulkScorer, collector, singleClauseCollector, acceptDocs, windowMin, windowMax, max);
      return head.add(bulkScorer);
    } else {
      // general case, collect through a bit set first and then replay
      scoreWindowMultipleScorers(collector, acceptDocs, windowBase, windowMin, windowMax, maxFreq);
      return head.top();
    }
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    fakeScorer.doc = -1;
    collector.setScorer(fakeScorer);

    final LeafCollector singleClauseCollector;
    if (coordFactors[1] == 1f) {
      singleClauseCollector = collector;
    } else {
      singleClauseCollector = new FilterLeafCollector(collector) {
        @Override
        public void setScorer(Scorer scorer) throws IOException {
          super.setScorer(new BooleanTopLevelScorers.BoostedScorer(scorer, coordFactors[1]));
        }
      };
    }

    BulkScorerAndDoc top = advance(min);
    while (top.next < max) {
      top = scoreWindow(top, collector, singleClauseCollector, acceptDocs, min, max);
    }

    return top.next;
  }

}
