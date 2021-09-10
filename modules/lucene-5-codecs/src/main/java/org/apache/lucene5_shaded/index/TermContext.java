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
package org.apache.lucene5_shaded.index;


import org.apache.lucene5_shaded.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;

/**
 * Maintains a {@link IndexReader} {@link TermState} view over
 * {@link IndexReader} instances containing a single term. The
 * {@link TermContext} doesn't track if the given {@link TermState}
 * objects are valid, neither if the {@link TermState} instances refer to the
 * same terms in the associated readers.
 * 
 * @lucene.experimental
 */
public final class TermContext {

  // Important: do NOT keep hard references to index readers
  private final Object topReaderContextIdentity;
  private final TermState[] states;
  private int docFreq;
  private long totalTermFreq;

  //public static boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  /**
   * Creates an empty {@link TermContext} from a {@link IndexReaderContext}
   */
  public TermContext(IndexReaderContext context) {
    assert context != null && context.isTopLevel;
    topReaderContextIdentity = context.identity;
    docFreq = 0;
    totalTermFreq = 0;
    final int len;
    if (context.leaves() == null) {
      len = 1;
    } else {
      len = context.leaves().size();
    }
    states = new TermState[len];
  }

  /**
   * Expert: Return whether this {@link TermContext} was built for the given
   * {@link IndexReaderContext}. This is typically used for assertions.
   * @lucene.internal
   */
  public boolean wasBuiltFor(IndexReaderContext context) {
    return topReaderContextIdentity == context.identity;
  }

  /**
   * Creates a {@link TermContext} with an initial {@link TermState},
   * {@link IndexReader} pair.
   */
  public TermContext(IndexReaderContext context, TermState state, int ord, int docFreq, long totalTermFreq) {
    this(context);
    register(state, ord, docFreq, totalTermFreq);
  }

  /**
   * Creates a {@link TermContext} from a top-level {@link IndexReaderContext} and the
   * given {@link Term}. This method will lookup the given term in all context's leaf readers 
   * and register each of the readers containing the term in the returned {@link TermContext}
   * using the leaf reader's ordinal.
   * <p>
   * Note: the given context must be a top-level context.
   */
  public static TermContext build(IndexReaderContext context, Term term)
      throws IOException {
    assert context != null && context.isTopLevel;
    final String field = term.field();
    final BytesRef bytes = term.bytes();
    final TermContext perReaderTermState = new TermContext(context);
    //if (DEBUG) System.out.println("prts.build term=" + term);
    for (final LeafReaderContext ctx : context.leaves()) {
      //if (DEBUG) System.out.println("  r=" + leaves[i].reader);
      final Terms terms = ctx.reader().terms(field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator();
        if (termsEnum.seekExact(bytes)) { 
          final TermState termState = termsEnum.termState();
          //if (DEBUG) System.out.println("    found");
          perReaderTermState.register(termState, ctx.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
        }
      }
    }
    return perReaderTermState;
  }

  /**
   * Clears the {@link TermContext} internal state and removes all
   * registered {@link TermState}s
   */
  public void clear() {
    docFreq = 0;
    totalTermFreq = 0;
    Arrays.fill(states, null);
  }

  /**
   * Registers and associates a {@link TermState} with an leaf ordinal. The leaf ordinal
   * should be derived from a {@link IndexReaderContext}'s leaf ord.
   */
  public void register(TermState state, final int ord, final int docFreq, final long totalTermFreq) {
    register(state, ord);
    accumulateStatistics(docFreq, totalTermFreq);
  }

  /**
   * Expert: Registers and associates a {@link TermState} with an leaf ordinal. The
   * leaf ordinal should be derived from a {@link IndexReaderContext}'s leaf ord.
   * On the contrary to {@link #register(TermState, int, int, long)} this method
   * does NOT update term statistics.
   */
  public void register(TermState state, final int ord) {
    assert state != null : "state must not be null";
    assert ord >= 0 && ord < states.length;
    assert states[ord] == null : "state for ord: " + ord
        + " already registered";
    states[ord] = state;
  }

  /** Expert: Accumulate term statistics. */
  public void accumulateStatistics(final int docFreq, final long totalTermFreq) {
    this.docFreq += docFreq;
    if (this.totalTermFreq >= 0 && totalTermFreq >= 0)
      this.totalTermFreq += totalTermFreq;
    else
      this.totalTermFreq = -1;
  }

  /**
   * Returns the {@link TermState} for an leaf ordinal or <code>null</code> if no
   * {@link TermState} for the ordinal was registered.
   * 
   * @param ord
   *          the readers leaf ordinal to get the {@link TermState} for.
   * @return the {@link TermState} for the given readers ord or <code>null</code> if no
   *         {@link TermState} for the reader was registered
   */
  public TermState get(int ord) {
    assert ord >= 0 && ord < states.length;
    return states[ord];
  }

  /**
   *  Returns the accumulated document frequency of all {@link TermState}
   *         instances passed to {@link #register(TermState, int, int, long)}.
   * @return the accumulated document frequency of all {@link TermState}
   *         instances passed to {@link #register(TermState, int, int, long)}.
   */
  public int docFreq() {
    return docFreq;
  }
  
  /**
   *  Returns the accumulated term frequency of all {@link TermState}
   *         instances passed to {@link #register(TermState, int, int, long)}.
   * @return the accumulated term frequency of all {@link TermState}
   *         instances passed to {@link #register(TermState, int, int, long)}.
   */
  public long totalTermFreq() {
    return totalTermFreq;
  }

  /** Returns true if all terms stored here are real (e.g., not auto-prefix terms).
   *
   *  @lucene.internal */
  public boolean hasOnlyRealTerms() {
    for (TermState termState : states) {
      if (termState != null && termState.isRealTerm() == false) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TermContext\n");
    for(TermState termState : states) {
      sb.append("  state=");
      sb.append(termState.toString());
      sb.append('\n');
    }

    return sb.toString();
  }
}
