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

import org.apache.lucene5_shaded.search.similarities.Similarity;
import org.apache.lucene5_shaded.search.TwoPhaseIterator;

/**
 * A Spans that wraps another Spans with a different SimScorer
 */
public class ScoringWrapperSpans extends Spans {

  private final Spans in;

  /**
   * Creates a new ScoringWrapperSpans
   * @param spans the scorer to wrap
   * @param simScorer  the SimScorer to use for scoring
   */
  public ScoringWrapperSpans(Spans spans, Similarity.SimScorer simScorer) {
    this.in = spans;
  }

  @Override
  public int nextStartPosition() throws IOException {
    return in.nextStartPosition();
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
  public TwoPhaseIterator asTwoPhaseIterator() {
    return in.asTwoPhaseIterator();
  }

  @Override
  public float positionsCost() {
    return in.positionsCost();
  }
}
