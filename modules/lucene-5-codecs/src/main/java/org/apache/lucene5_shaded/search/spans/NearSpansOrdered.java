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
import java.util.List;

/**
 * A Spans that is formed from the ordered subspans of a SpanNearQuery
 * where the subspans do not overlap and have a maximum slop between them.
 * <p>
 * The formed spans only contains minimum slop matches.<br>
 * The matching slop is computed from the distance(s) between
 * the non overlapping matching Spans.<br>
 * Successive matches are always formed from the successive Spans
 * of the SpanNearQuery.
 * <p>
 * The formed spans may contain overlaps when the slop is at least 1.
 * For example, when querying using
 * <pre>t1 t2 t3</pre>
 * with slop at least 1, the fragment:
 * <pre>t1 t2 t1 t3 t2 t3</pre>
 * matches twice:
 * <pre>t1 t2 .. t3      </pre>
 * <pre>      t1 .. t2 t3</pre>
 *
 * Expert:
 * Only public for subclassing.  Most implementations should not need this class
 */
public class NearSpansOrdered extends ConjunctionSpans {

  protected int matchStart = -1;
  protected int matchEnd = -1;
  protected int matchWidth = -1;

  private final int allowedSlop;

  public NearSpansOrdered(int allowedSlop, List<Spans> subSpans) throws IOException {
    super(subSpans);
    this.atFirstInCurrentDoc = true; // -1 startPosition/endPosition also at doc -1
    this.allowedSlop = allowedSlop;
  }

  @Override
  boolean twoPhaseCurrentDocMatches() throws IOException {
    assert unpositioned();
    oneExhaustedInCurrentDoc = false;
    while (subSpans[0].nextStartPosition() != NO_MORE_POSITIONS && !oneExhaustedInCurrentDoc) {
      if (stretchToOrder() && matchWidth <= allowedSlop) {
        return atFirstInCurrentDoc = true;
      }
    }
    return false;
  }

  private boolean unpositioned() {
    for (Spans span : subSpans) {
      if (span.startPosition() != -1)
        return false;
    }
    return true;
  }

  @Override
  public int nextStartPosition() throws IOException {
    if (atFirstInCurrentDoc) {
      atFirstInCurrentDoc = false;
      return matchStart;
    }
    oneExhaustedInCurrentDoc = false;
    while (subSpans[0].nextStartPosition() != NO_MORE_POSITIONS && !oneExhaustedInCurrentDoc) {
      if (stretchToOrder() && matchWidth <= allowedSlop) {
        return matchStart;
      }
    }
    return matchStart = matchEnd = NO_MORE_POSITIONS;
  }

  /**
   * Order the subSpans within the same document by using nextStartPosition on all subSpans
   * after the first as little as necessary.
   * Return true when the subSpans could be ordered in this way,
   * otherwise at least one is exhausted in the current doc.
   */
  private boolean stretchToOrder() throws IOException {
    Spans prevSpans = subSpans[0];
    matchStart = prevSpans.startPosition();
    assert prevSpans.startPosition() != NO_MORE_POSITIONS : "prevSpans no start position "+prevSpans;
    assert prevSpans.endPosition() != NO_MORE_POSITIONS;
    matchWidth = 0;
    for (int i = 1; i < subSpans.length; i++) {
      Spans spans = subSpans[i];
      assert spans.startPosition() != NO_MORE_POSITIONS;
      assert spans.endPosition() != NO_MORE_POSITIONS;
      if (advancePosition(spans, prevSpans.endPosition()) == NO_MORE_POSITIONS) {
        oneExhaustedInCurrentDoc = true;
        return false;
      }
      matchWidth += (spans.startPosition() - prevSpans.endPosition());
      prevSpans = spans;
    }
    matchEnd = subSpans[subSpans.length - 1].endPosition();
    return true; // all subSpans ordered and non overlapping
  }

  private static int advancePosition(Spans spans, int position) throws IOException {
    if (spans instanceof SpanNearQuery.GapSpans) {
      return ((SpanNearQuery.GapSpans)spans).skipToPosition(position);
    }
    while (spans.startPosition() < position) {
      spans.nextStartPosition();
    }
    return spans.startPosition();
  }

  @Override
  public int startPosition() {
    return atFirstInCurrentDoc ? -1 : matchStart;
  }

  @Override
  public int endPosition() {
    return atFirstInCurrentDoc ? -1 : matchEnd;
  }

  @Override
  public int width() {
    return matchWidth;
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    for (Spans spans : subSpans) {
      spans.collect(collector);
    }
  }

}

