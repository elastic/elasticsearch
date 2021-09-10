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


import java.io.IOException;

import org.apache.lucene5_shaded.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;
import org.apache.lucene5_shaded.util.automaton.CompiledAutomaton;

/**
 * Access to the terms in a specific field.  See {@link Fields}.
 * @lucene.experimental
 */

public abstract class Terms {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected Terms() {
  }

  /** Returns an iterator that will step through all
   *  terms. This method will not return null. */
  public abstract TermsEnum iterator() throws IOException;

  /** Returns a TermsEnum that iterates over all terms and
   *  documents that are accepted by the provided {@link
   *  CompiledAutomaton}.  If the <code>startTerm</code> is
   *  provided then the returned enum will only return terms
   *  {@code > startTerm}, but you still must call
   *  next() first to get to the first term.  Note that the
   *  provided <code>startTerm</code> must be accepted by
   *  the automaton.
   *
   * <p><b>NOTE</b>: the returned TermsEnum cannot
   * seek</p>.
   *
   *  <p><b>NOTE</b>: the terms dictionary is free to
   *  return arbitrary terms as long as the resulted visited
   *  docs is the same.  E.g., {@link BlockTreeTermsWriter}
   *  creates auto-prefix terms during indexing to reduce the
   *  number of terms visited. */
  public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm) throws IOException {
    
    // TODO: could we factor out a common interface b/w
    // CompiledAutomaton and FST?  Then we could pass FST there too,
    // and likely speed up resolving terms to deleted docs ... but
    // AutomatonTermsEnum makes this tricky because of its on-the-fly cycle
    // detection
    
    // TODO: eventually we could support seekCeil/Exact on
    // the returned enum, instead of only being able to seek
    // at the start

    TermsEnum termsEnum = iterator();

    if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
      throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
    }

    if (startTerm == null) {
      return new AutomatonTermsEnum(termsEnum, compiled);
    } else {
      return new AutomatonTermsEnum(termsEnum, compiled) {
        @Override
        protected BytesRef nextSeekTerm(BytesRef term) throws IOException {
          if (term == null) {
            term = startTerm;
          }
          return super.nextSeekTerm(term);
        }
      };
    }
  }

  /** Returns the number of terms for this field, or -1 if this 
   *  measure isn't stored by the codec. Note that, just like 
   *  other term measures, this measure does not take deleted 
   *  documents into account. */
  public abstract long size() throws IOException;
  
  /** Returns the sum of {@link TermsEnum#totalTermFreq} for
   *  all terms in this field, or -1 if this measure isn't
   *  stored by the codec (or if this fields omits term freq
   *  and positions).  Note that, just like other term
   *  measures, this measure does not take deleted documents
   *  into account. */
  public abstract long getSumTotalTermFreq() throws IOException;

  /** Returns the sum of {@link TermsEnum#docFreq()} for
   *  all terms in this field, or -1 if this measure isn't
   *  stored by the codec.  Note that, just like other term
   *  measures, this measure does not take deleted documents
   *  into account. */
  public abstract long getSumDocFreq() throws IOException;

  /** Returns the number of documents that have at least one
   *  term for this field, or -1 if this measure isn't
   *  stored by the codec.  Note that, just like other term
   *  measures, this measure does not take deleted documents
   *  into account. */
  public abstract int getDocCount() throws IOException;

  /** Returns true if documents in this field store
   *  per-document term frequency ({@link PostingsEnum#freq}). */
  public abstract boolean hasFreqs();

  /** Returns true if documents in this field store offsets. */
  public abstract boolean hasOffsets();
  
  /** Returns true if documents in this field store positions. */
  public abstract boolean hasPositions();
  
  /** Returns true if documents in this field store payloads. */
  public abstract boolean hasPayloads();

  /** Zero-length array of {@link Terms}. */
  public final static Terms[] EMPTY_ARRAY = new Terms[0];
  
  /** Returns the smallest term (in lexicographic order) in the field. 
   *  Note that, just like other term measures, this measure does not 
   *  take deleted documents into account.  This returns
   *  null when there are no terms. */
  public BytesRef getMin() throws IOException {
    return iterator().next();
  }

  /** Returns the largest term (in lexicographic order) in the field. 
   *  Note that, just like other term measures, this measure does not 
   *  take deleted documents into account.  This returns
   *  null when there are no terms. */
  @SuppressWarnings("fallthrough")
  public BytesRef getMax() throws IOException {
    long size = size();
    
    if (size == 0) {
      // empty: only possible from a FilteredTermsEnum...
      return null;
    } else if (size >= 0) {
      // try to seek-by-ord
      try {
        TermsEnum iterator = iterator();
        iterator.seekExact(size - 1);
        return iterator.term();
      } catch (UnsupportedOperationException e) {
        // ok
      }
    }
    
    // otherwise: binary search
    TermsEnum iterator = iterator();
    BytesRef v = iterator.next();
    if (v == null) {
      // empty: only possible from a FilteredTermsEnum...
      return v;
    }

    BytesRefBuilder scratch = new BytesRefBuilder();
    scratch.append((byte) 0);

    // Iterates over digits:
    while (true) {

      int low = 0;
      int high = 256;

      // Binary search current digit to find the highest
      // digit before END:
      while (low != high) {
        int mid = (low+high) >>> 1;
        scratch.setByteAt(scratch.length()-1, (byte) mid);
        if (iterator.seekCeil(scratch.get()) == TermsEnum.SeekStatus.END) {
          // Scratch was too high
          if (mid == 0) {
            scratch.setLength(scratch.length() - 1);
            return scratch.get();
          }
          high = mid;
        } else {
          // Scratch was too low; there is at least one term
          // still after it:
          if (low == mid) {
            break;
          }
          low = mid;
        }
      }

      // Recurse to next digit:
      scratch.setLength(scratch.length() + 1);
      scratch.grow(scratch.length());
    }
  }
  
  /** 
   * Expert: returns additional information about this Terms instance
   * for debugging purposes.
   */
  public Object getStats() throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("impl=" + getClass().getSimpleName());
    sb.append(",size=" + size());
    sb.append(",docCount=" + getDocCount());
    sb.append(",sumTotalTermFreq=" + getSumTotalTermFreq());
    sb.append(",sumDocFreq=" + getSumDocFreq());
    return sb.toString();
  }
}
