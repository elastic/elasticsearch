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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene5_shaded.util.PriorityQueue;

import static org.apache.lucene5_shaded.search.DisiPriorityQueue.leftNode;
import static org.apache.lucene5_shaded.search.DisiPriorityQueue.parentNode;
import static org.apache.lucene5_shaded.search.DisiPriorityQueue.rightNode;

/**
 * A {@link Scorer} for {@link BooleanQuery} when
 * {@link BooleanQuery.Builder#setMinimumNumberShouldMatch(int) minShouldMatch} is
 * between 2 and the total number of clauses.
 *
 * This implementation keeps sub scorers in 3 different places:
 *  - lead: a linked list of scorer that are positioned on the desired doc ID
 *  - tail: a heap that contains at most minShouldMatch - 1 scorers that are
 *    behind the desired doc ID. These scorers are ordered by cost so that we
 *    can advance the least costly ones first.
 *  - head: a heap that contains scorers which are beyond the desired doc ID,
 *    ordered by doc ID in order to move quickly to the next candidate.
 *
 * Finding the next match consists of first setting the desired doc ID to the
 * least entry in 'head' and then advance 'tail' until there is a match.
 */
final class MinShouldMatchSumScorer extends Scorer {

  private static long cost(Collection<Scorer> scorers, int minShouldMatch) {
    // the idea here is the following: a boolean query c1,c2,...cn with minShouldMatch=m
    // could be rewritten to:
    // (c1 AND (c2..cn|msm=m-1)) OR (!c1 AND (c2..cn|msm=m))
    // if we assume that clauses come in ascending cost, then
    // the cost of the first part is the cost of c1 (because the cost of a conjunction is
    // the cost of the least costly clause)
    // the cost of the second part is the cost of finding m matches among the c2...cn
    // remaining clauses
    // since it is a disjunction overall, the total cost is the sum of the costs of these
    // two parts

    // If we recurse infinitely, we find out that the cost of a msm query is the sum of the
    // costs of the num_scorers - minShouldMatch + 1 least costly scorers
    final PriorityQueue<Scorer> pq = new PriorityQueue<Scorer>(scorers.size() - minShouldMatch + 1) {
      @Override
      protected boolean lessThan(Scorer a, Scorer b) {
        return a.iterator().cost() > b.iterator().cost();
      }
    };
    for (Scorer scorer : scorers) {
      pq.insertWithOverflow(scorer);
    }
    long cost = 0;
    for (Scorer scorer = pq.pop(); scorer != null; scorer = pq.pop()) {
      cost += scorer.iterator().cost();
    }
    return cost;
  }

  final int minShouldMatch;
  final float[] coord;

  // list of scorers which 'lead' the iteration and are currently
  // positioned on 'doc'
  DisiWrapper lead;
  int doc;  // current doc ID of the leads
  int freq; // number of scorers on the desired doc ID

  // priority queue of scorers that are too advanced compared to the current
  // doc. Ordered by doc ID.
  final DisiPriorityQueue head;

  // priority queue of scorers which are behind the current doc.
  // Ordered by cost.
  final DisiWrapper[] tail;
  int tailSize;

  final Collection<ChildScorer> childScorers;
  final long cost;

  MinShouldMatchSumScorer(Weight weight, Collection<Scorer> scorers, int minShouldMatch, float[] coord) {
    super(weight);

    if (minShouldMatch > scorers.size()) {
      throw new IllegalArgumentException("minShouldMatch should be <= the number of scorers");
    }
    if (minShouldMatch < 1) {
      throw new IllegalArgumentException("minShouldMatch should be >= 1");
    }

    this.minShouldMatch = minShouldMatch;
    this.coord = coord;
    this.doc = -1;

    head = new DisiPriorityQueue(scorers.size() - minShouldMatch + 1);
    // there can be at most minShouldMatch - 1 scorers beyond the current position
    // otherwise we might be skipping over matching documents
    tail = new DisiWrapper[minShouldMatch - 1];

    for (Scorer scorer : scorers) {
      addLead(new DisiWrapper(scorer));
    }

    List<ChildScorer> children = new ArrayList<>();
    for (Scorer scorer : scorers) {
      children.add(new ChildScorer(scorer, "SHOULD"));
    }
    this.childScorers = Collections.unmodifiableCollection(children);
    this.cost = cost(scorers, minShouldMatch);
  }

  @Override
  public final Collection<ChildScorer> getChildren() {
    return childScorers;
  }

  @Override
  public DocIdSetIterator iterator() {
    return new DocIdSetIterator() {

      @Override
      public int docID() {
        assert doc == lead.doc;
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        // We are moving to the next doc ID, so scorers in 'lead' need to go in
        // 'tail'. If there is not enough space in 'tail', then we take the least
        // costly scorers and advance them.
        for (DisiWrapper s = lead; s != null; s = s.next) {
          final DisiWrapper evicted = insertTailWithOverFlow(s);
          if (evicted != null) {
            if (evicted.doc == doc) {
              evicted.doc = evicted.iterator.nextDoc();
            } else {
              evicted.doc = evicted.iterator.advance(doc + 1);
            }
            head.add(evicted);
          }
        }

        setDocAndFreq();
        return doNext();
      }

      @Override
      public int advance(int target) throws IOException {
        // Same logic as in nextDoc
        for (DisiWrapper s = lead; s != null; s = s.next) {
          final DisiWrapper evicted = insertTailWithOverFlow(s);
          if (evicted != null) {
            evicted.doc = evicted.iterator.advance(target);
            head.add(evicted);
          }
        }

        // But this time there might also be scorers in 'head' behind the desired
        // target so we need to do the same thing that we did on 'lead' on 'head'
        DisiWrapper headTop = head.top();
        while (headTop.doc < target) {
          final DisiWrapper evicted = insertTailWithOverFlow(headTop);
          // We know that the tail is full since it contains at most
          // minShouldMatch - 1 entries and we just moved at least minShouldMatch
          // entries to it, so evicted is not null
          evicted.doc = evicted.iterator.advance(target);
          headTop = head.updateTop(evicted);
        }

        setDocAndFreq();
        return doNext();
      }

      @Override
      public long cost() {
        return cost;
      }
    };
  }

  private void addLead(DisiWrapper lead) {
    lead.next = this.lead;
    this.lead = lead;
    freq += 1;
  }

  private void pushBackLeads() throws IOException {
    for (DisiWrapper s = lead; s != null; s = s.next) {
      addTail(s);
    }
  }

  private void advanceTail(DisiWrapper top) throws IOException {
    top.doc = top.iterator.advance(doc);
    if (top.doc == doc) {
      addLead(top);
    } else {
      head.add(top);
    }
  }

  private void advanceTail() throws IOException {
    final DisiWrapper top = popTail();
    advanceTail(top);
  }

  /** Reinitializes head, freq and doc from 'head' */
  private void setDocAndFreq() {
    assert head.size() > 0;

    // The top of `head` defines the next potential match
    // pop all documents which are on this doc
    lead = head.pop();
    lead.next = null;
    freq = 1;
    doc = lead.doc;
    while (head.size() > 0 && head.top().doc == doc) {
      addLead(head.pop());
    }
  }

  /** Advance tail to the lead until there is a match. */
  private int doNext() throws IOException {
    while (freq < minShouldMatch) {
      assert freq > 0;
      if (freq + tailSize >= minShouldMatch) {
        // a match on doc is still possible, try to
        // advance scorers from the tail
        advanceTail();
      } else {
        // no match on doc is possible anymore, move to the next potential match
        pushBackLeads();
        setDocAndFreq();
      }
    }

    return doc;
  }

  /** Advance all entries from the tail to know about all matches on the
   *  current doc. */
  private void updateFreq() throws IOException {
    assert freq >= minShouldMatch;
    // we return the next doc when there are minShouldMatch matching clauses
    // but some of the clauses in 'tail' might match as well
    // in general we want to advance least-costly clauses first in order to
    // skip over non-matching documents as fast as possible. However here,
    // we are advancing everything anyway so iterating over clauses in
    // (roughly) cost-descending order might help avoid some permutations in
    // the head heap
    for (int i = tailSize - 1; i >= 0; --i) {
      advanceTail(tail[i]);
    }
    tailSize = 0;
  }

  @Override
  public int freq() throws IOException {
    // we need to know about all matches
    updateFreq();
    return freq;
  }

  @Override
  public float score() throws IOException {
    // we need to know about all matches
    updateFreq();
    double score = 0;
    for (DisiWrapper s = lead; s != null; s = s.next) {
      score += s.scorer.score();
    }
    return coord[freq] * (float) score;
  }

  @Override
  public int docID() {
    assert doc == lead.doc;
    return doc;
  }

  /** Insert an entry in 'tail' and evict the least-costly scorer if full. */
  private DisiWrapper insertTailWithOverFlow(DisiWrapper s) {
    if (tailSize < tail.length) {
      addTail(s);
      return null;
    } else if (tail.length >= 1) {
      final DisiWrapper top = tail[0];
      if (top.cost < s.cost) {
        tail[0] = s;
        downHeapCost(tail, tailSize);
        return top;
      }
    }
    return s;
  }

  /** Add an entry to 'tail'. Fails if over capacity. */
  private void addTail(DisiWrapper s) {
    tail[tailSize] = s;
    upHeapCost(tail, tailSize);
    tailSize += 1;
  }

  /** Pop the least-costly scorer from 'tail'. */
  private DisiWrapper popTail() {
    assert tailSize > 0;
    final DisiWrapper result = tail[0];
    tail[0] = tail[--tailSize];
    downHeapCost(tail, tailSize);
    return result;
  }

  /** Heap helpers */

  private static void upHeapCost(DisiWrapper[] heap, int i) {
    final DisiWrapper node = heap[i];
    final long nodeCost = node.cost;
    int j = parentNode(i);
    while (j >= 0 && nodeCost < heap[j].cost) {
      heap[i] = heap[j];
      i = j;
      j = parentNode(j);
    }
    heap[i] = node;
  }

  private static void downHeapCost(DisiWrapper[] heap, int size) {
    int i = 0;
    final DisiWrapper node = heap[0];
    int j = leftNode(i);
    if (j < size) {
      int k = rightNode(j);
      if (k < size && heap[k].cost < heap[j].cost) {
        j = k;
      }
      if (heap[j].cost < node.cost) {
        do {
          heap[i] = heap[j];
          i = j;
          j = leftNode(i);
          k = rightNode(j);
          if (k < size && heap[k].cost < heap[j].cost) {
            j = k;
          }
        } while (j < size && heap[j].cost < node.cost);
        heap[i] = node;
      }
    }
  }

}
