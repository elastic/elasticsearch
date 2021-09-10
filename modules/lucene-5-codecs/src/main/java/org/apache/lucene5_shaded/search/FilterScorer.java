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

/** 
 * A {@code FilterScorer} contains another {@code Scorer}, which it
 * uses as its basic source of data, possibly transforming the data along the
 * way or providing additional functionality. The class
 * {@code FilterScorer} itself simply implements all abstract methods
 * of {@code Scorer} with versions that pass all requests to the
 * contained scorer. Subclasses of {@code FilterScorer} may
 * further override some of these methods and may also provide additional
 * methods and fields.
 */
public abstract class FilterScorer extends Scorer {
  protected final Scorer in;

  /**
   * Create a new FilterScorer
   * @param in the {@link Scorer} to wrap
   */
  public FilterScorer(Scorer in) {
    super(in.weight);
    this.in = in;
  }

  /**
   * Create a new FilterScorer with a specific weight
   * @param in the {@link Scorer} to wrap
   * @param weight a {@link Weight}
   */
  public FilterScorer(Scorer in, Weight weight) {
    super(weight);
    if (in == null) {
      throw new NullPointerException("wrapped Scorer must not be null");
    }
    this.in = in;
  }
  
  @Override
  public float score() throws IOException {
    return in.score();
  }

  @Override
  public int freq() throws IOException {
    return in.freq();
  }

  @Override
  public final int docID() {
    return in.docID();
  }

  @Override
  public final DocIdSetIterator iterator() {
    return in.iterator();
  }
  
  @Override
  public final TwoPhaseIterator twoPhaseIterator() {
    return in.twoPhaseIterator();
  }
}
