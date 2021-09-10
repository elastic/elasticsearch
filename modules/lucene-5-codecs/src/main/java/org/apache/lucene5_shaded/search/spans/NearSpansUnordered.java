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
package org.apache.lucene5_shaded.search.spans;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene5_shaded.search.TwoPhaseIterator;
import org.apache.lucene5_shaded.util.PriorityQueue;

/**
 * Similar to {@link NearSpansOrdered}, but for the unordered case.
 *
 * Expert:
 * Only public for subclassing.  Most implementations should not need this class
 */
public class NearSpansUnordered extends ConjunctionSpans {

  private List<SpansCell> subSpanCells; // in query order
  private final int allowedSlop;

  private SpanPositionQueue spanPositionQueue;

  public NearSpansUnordered(int allowedSlop, List<Spans> subSpans)
  throws IOException {
    super(subSpans);

    this.subSpanCells = new ArrayList<>(subSpans.size());
    for (Spans subSpan : subSpans) { // sub spans in query order
      this.subSpanCells.add(new SpansCell(subSpan));
    }
    spanPositionQueue = new SpanPositionQueue(subSpans.size());
    singleCellToPositionQueue(); // -1 startPosition/endPosition also at doc -1
    this.allowedSlop = allowedSlop;
  }

  private void singleCellToPositionQueue() {
    maxEndPositionCell = subSpanCells.get(0);
    assert maxEndPositionCell.docID() == -1;
    assert maxEndPositionCell.startPosition() == -1;
    spanPositionQueue.add(maxEndPositionCell);
  }

  private void subSpanCellsToPositionQueue() throws IOException { // used when all subSpanCells arrived at the same doc.
    spanPositionQueue.clear();
    for (SpansCell cell : subSpanCells) {
      assert cell.startPosition() == -1;
      cell.nextStartPosition();
      assert cell.startPosition() != NO_MORE_POSITIONS;
      spanPositionQueue.add(cell);
    }
  }

  /** SpansCell wraps a sub Spans to maintain totalSpanLength and maxEndPositionCell */
  private int totalSpanLength;
  private SpansCell maxEndPositionCell;

  private class SpansCell extends Spans {
    private int spanLength = -1;
    final Spans in;

    public SpansCell(Spans spans) {
      this.in = spans;
    }

    @Override
    public int nextStartPosition() throws IOException {
      int res = in.nextStartPosition();
      if (res != NO_MORE_POSITIONS) {
        adjustLength();
      }
      adjustMax(); // also after last end position in current doc.
      return res;
    }

    private void adjustLength() {
      if (spanLength != -1) {
        totalSpanLength -= spanLength;  // subtract old, possibly from a previous doc
      }
      assert in.startPosition() != NO_MORE_POSITIONS;
      spanLength = endPosition() - startPosition();
      assert spanLength >= 0;
      totalSpanLength += spanLength; // add new
    }

    private void adjustMax() {
      assert docID() == maxEndPositionCell.docID();
      if (endPosition() > maxEndPositionCell.endPosition()) {
        maxEndPositionCell = this;
      }
    }

    @Override
    public int startPosition() {
      return in.startPosition();
    }

    @Override
    public int endPosition() {
      return in.endPosition();
    }

    @Override
    public int width() {
      return in.width();
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {
      in.collect(collector);
    }

    @Override
    public TwoPhaseIterator asTwoPhaseIterator() {
      return in.asTwoPhaseIterator();
    }

    @Override
    public float positionsCost() {
      return in.positionsCost();
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return in.advance(target);
    }

    @Override
    public long cost() {
      return in.cost();
    }

    @Override
    public String toString() {
      return "NearSpansUnordered.SpansCell(" + in.toString() + ")";
    }
  }


  private static class SpanPositionQueue extends PriorityQueue<SpansCell> {
    public SpanPositionQueue(int size) {
      super(size);
    }

    @Override
    protected final boolean lessThan(SpansCell spans1, SpansCell spans2) {
      return positionsOrdered(spans1, spans2);
    }
  }

  /** Check whether two Spans in the same document are ordered with possible overlap.
   * @return true iff spans1 starts before spans2
   *              or the spans start at the same position,
   *              and spans1 ends before spans2.
   */
  static boolean positionsOrdered(Spans spans1, Spans spans2) {
    assert spans1.docID() == spans2.docID() : "doc1 " + spans1.docID() + " != doc2 " + spans2.docID();
    int start1 = spans1.startPosition();
    int start2 = spans2.startPosition();
    return (start1 == start2) ? (spans1.endPosition() < spans2.endPosition()) : (start1 < start2);
  }

  private SpansCell minPositionCell() {
    return spanPositionQueue.top();
  }

  private boolean atMatch() {
    assert minPositionCell().docID() == maxEndPositionCell.docID();
    return (maxEndPositionCell.endPosition() - minPositionCell().startPosition() - totalSpanLength) <= allowedSlop;
  }

  @Override
  boolean twoPhaseCurrentDocMatches() throws IOException {
    // at doc with all subSpans
    subSpanCellsToPositionQueue();
    while (true) {
      if (atMatch()) {
        atFirstInCurrentDoc = true;
        oneExhaustedInCurrentDoc = false;
        return true;
      }
      assert minPositionCell().startPosition() != NO_MORE_POSITIONS;
      if (minPositionCell().nextStartPosition() != NO_MORE_POSITIONS) {
        spanPositionQueue.updateTop();
      }
      else { // exhausted a subSpan in current doc
        return false;
      }
    }
  }

  @Override
  public int nextStartPosition() throws IOException {
    if (atFirstInCurrentDoc) {
      atFirstInCurrentDoc = false;
      return minPositionCell().startPosition();
    }
    while (minPositionCell().startPosition() == -1) { // initially at current doc
      minPositionCell().nextStartPosition();
      spanPositionQueue.updateTop();
    }
    assert minPositionCell().startPosition() != NO_MORE_POSITIONS;
    while (true) {
      if (minPositionCell().nextStartPosition() == NO_MORE_POSITIONS) {
        oneExhaustedInCurrentDoc = true;
        return NO_MORE_POSITIONS;
      }
      spanPositionQueue.updateTop();
      if (atMatch()) {
        return minPositionCell().startPosition();
      }
    }
  }

  @Override
  public int startPosition() {
    assert minPositionCell() != null;
    return atFirstInCurrentDoc ? -1
          : oneExhaustedInCurrentDoc ? NO_MORE_POSITIONS
          : minPositionCell().startPosition();
  }

  @Override
  public int endPosition() {
    return atFirstInCurrentDoc ? -1
          : oneExhaustedInCurrentDoc ? NO_MORE_POSITIONS
          : maxEndPositionCell.endPosition();
  }

  @Override
  public int width() {
    return maxEndPositionCell.startPosition() - minPositionCell().startPosition();
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    for (SpansCell cell : subSpanCells) {
      cell.collect(collector);
    }
  }

}
