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
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;
import org.apache.lucene5_shaded.util.PriorityQueue;

/**
 * Exposes {@link TermsEnum} API, merged from {@link TermsEnum} API of sub-segments.
 * This does a merge sort, by term text, of the sub-readers.
 *
 * @lucene.experimental
 */
public final class MultiTermsEnum extends TermsEnum {

  private static final Comparator<TermsEnumWithSlice> INDEX_COMPARATOR = new Comparator<TermsEnumWithSlice>() {
    @Override
    public int compare(TermsEnumWithSlice o1, TermsEnumWithSlice o2) {
      return o1.index - o2.index;
    }
  };

  private final TermMergeQueue queue;
  private final TermsEnumWithSlice[] subs;        // all of our subs (one per sub-reader)
  private final TermsEnumWithSlice[] currentSubs; // current subs that have at least one term for this field
  private final TermsEnumWithSlice[] top;
  private final MultiPostingsEnum.EnumWithSlice[] subDocs;

  private BytesRef lastSeek;
  private boolean lastSeekExact;
  private final BytesRefBuilder lastSeekScratch = new BytesRefBuilder();

  private int numTop;
  private int numSubs;
  private BytesRef current;

  static class TermsEnumIndex {
    public final static TermsEnumIndex[] EMPTY_ARRAY = new TermsEnumIndex[0];
    final int subIndex;
    final TermsEnum termsEnum;

    public TermsEnumIndex(TermsEnum termsEnum, int subIndex) {
      this.termsEnum = termsEnum;
      this.subIndex = subIndex;
    }
  }

  /** Returns how many sub-reader slices contain the current
   *  term.  @see #getMatchArray */
  public int getMatchCount() {
    return numTop;
  }

  /** Returns sub-reader slices positioned to the current term. */
  public TermsEnumWithSlice[] getMatchArray() {
    return top;
  }

  /** Sole constructor.
   *  @param slices Which sub-reader slices we should
   *  merge. */
  public MultiTermsEnum(ReaderSlice[] slices) {
    queue = new TermMergeQueue(slices.length);
    top = new TermsEnumWithSlice[slices.length];
    subs = new TermsEnumWithSlice[slices.length];
    subDocs = new MultiPostingsEnum.EnumWithSlice[slices.length];
    for(int i=0;i<slices.length;i++) {
      subs[i] = new TermsEnumWithSlice(i, slices[i]);
      subDocs[i] = new MultiPostingsEnum.EnumWithSlice();
      subDocs[i].slice = slices[i];
    }
    currentSubs = new TermsEnumWithSlice[slices.length];
  }

  @Override
  public BytesRef term() {
    return current;
  }

  /** The terms array must be newly created TermsEnum, ie
   *  {@link TermsEnum#next} has not yet been called. */
  public TermsEnum reset(TermsEnumIndex[] termsEnumsIndex) throws IOException {
    assert termsEnumsIndex.length <= top.length;
    numSubs = 0;
    numTop = 0;
    queue.clear();
    for(int i=0;i<termsEnumsIndex.length;i++) {

      final TermsEnumIndex termsEnumIndex = termsEnumsIndex[i];
      assert termsEnumIndex != null;

      final BytesRef term = termsEnumIndex.termsEnum.next();
      if (term != null) {
        final TermsEnumWithSlice entry = subs[termsEnumIndex.subIndex];
        entry.reset(termsEnumIndex.termsEnum, term);
        queue.add(entry);
        currentSubs[numSubs++] = entry;
      } else {
        // field has no terms
      }
    }

    if (queue.size() == 0) {
      return TermsEnum.EMPTY;
    } else {
      return this;
    }
  }

  @Override
  public boolean seekExact(BytesRef term) throws IOException {
    queue.clear();
    numTop = 0;

    boolean seekOpt = false;
    if (lastSeek != null && lastSeek.compareTo(term) <= 0) {
      seekOpt = true;
    }

    lastSeek = null;
    lastSeekExact = true;

    for(int i=0;i<numSubs;i++) {
      final boolean status;
      // LUCENE-2130: if we had just seek'd already, prior
      // to this seek, and the new seek term is after the
      // previous one, don't try to re-seek this sub if its
      // current term is already beyond this new seek term.
      // Doing so is a waste because this sub will simply
      // seek to the same spot.
      if (seekOpt) {
        final BytesRef curTerm = currentSubs[i].current;
        if (curTerm != null) {
          final int cmp = term.compareTo(curTerm);
          if (cmp == 0) {
            status = true;
          } else if (cmp < 0) {
            status = false;
          } else {
            status = currentSubs[i].terms.seekExact(term);
          }
        } else {
          status = false;
        }
      } else {
        status = currentSubs[i].terms.seekExact(term);
      }

      if (status) {
        top[numTop++] = currentSubs[i];
        current = currentSubs[i].current = currentSubs[i].terms.term();
        assert term.equals(currentSubs[i].current);
      }
    }

    // if at least one sub had exact match to the requested
    // term then we found match
    return numTop > 0;
  }

  @Override
  public SeekStatus seekCeil(BytesRef term) throws IOException {
    queue.clear();
    numTop = 0;
    lastSeekExact = false;

    boolean seekOpt = false;
    if (lastSeek != null && lastSeek.compareTo(term) <= 0) {
      seekOpt = true;
    }

    lastSeekScratch.copyBytes(term);
    lastSeek = lastSeekScratch.get();

    for(int i=0;i<numSubs;i++) {
      final SeekStatus status;
      // LUCENE-2130: if we had just seek'd already, prior
      // to this seek, and the new seek term is after the
      // previous one, don't try to re-seek this sub if its
      // current term is already beyond this new seek term.
      // Doing so is a waste because this sub will simply
      // seek to the same spot.
      if (seekOpt) {
        final BytesRef curTerm = currentSubs[i].current;
        if (curTerm != null) {
          final int cmp = term.compareTo(curTerm);
          if (cmp == 0) {
            status = SeekStatus.FOUND;
          } else if (cmp < 0) {
            status = SeekStatus.NOT_FOUND;
          } else {
            status = currentSubs[i].terms.seekCeil(term);
          }
        } else {
          status = SeekStatus.END;
        }
      } else {
        status = currentSubs[i].terms.seekCeil(term);
      }

      if (status == SeekStatus.FOUND) {
        top[numTop++] = currentSubs[i];
        current = currentSubs[i].current = currentSubs[i].terms.term();
        queue.add(currentSubs[i]);
      } else {
        if (status == SeekStatus.NOT_FOUND) {
          currentSubs[i].current = currentSubs[i].terms.term();
          assert currentSubs[i].current != null;
          queue.add(currentSubs[i]);
        } else {
          assert status == SeekStatus.END;
          // enum exhausted
          currentSubs[i].current = null;
        }
      }
    }

    if (numTop > 0) {
      // at least one sub had exact match to the requested term
      return SeekStatus.FOUND;
    } else if (queue.size() > 0) {
      // no sub had exact match, but at least one sub found
      // a term after the requested term -- advance to that
      // next term:
      pullTop();
      return SeekStatus.NOT_FOUND;
    } else {
      return SeekStatus.END;
    }
  }

  @Override
  public void seekExact(long ord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long ord() {
    throw new UnsupportedOperationException();
  }

  private void pullTop() {
    // extract all subs from the queue that have the same
    // top term
    assert numTop == 0;
    numTop = queue.fillTop(top);
    current = top[0].current;
  }

  private void pushTop() throws IOException {
    // call next() on each top, and reorder queue
    for (int i = 0; i < numTop; i++) {
      TermsEnumWithSlice top = queue.top();
      top.current = top.terms.next();
      if (top.current == null) {
        queue.pop();
      } else {
        queue.updateTop();
      }
    }
    numTop = 0;
  }

  @Override
  public BytesRef next() throws IOException {
    if (lastSeekExact) {
      // Must seekCeil at this point, so those subs that
      // didn't have the term can find the following term.
      // NOTE: we could save some CPU by only seekCeil the
      // subs that didn't match the last exact seek... but
      // most impls short-circuit if you seekCeil to term
      // they are already on.
      final SeekStatus status = seekCeil(current);
      assert status == SeekStatus.FOUND;
      lastSeekExact = false;
    }
    lastSeek = null;

    // restore queue
    pushTop();

    // gather equal top fields
    if (queue.size() > 0) {
      pullTop();
    } else {
      current = null;
    }

    return current;
  }

  @Override
  public int docFreq() throws IOException {
    int sum = 0;
    for(int i=0;i<numTop;i++) {
      sum += top[i].terms.docFreq();
    }
    return sum;
  }

  @Override
  public long totalTermFreq() throws IOException {
    long sum = 0;
    for(int i=0;i<numTop;i++) {
      final long v = top[i].terms.totalTermFreq();
      if (v == -1) {
        return v;
      }
      sum += v;
    }
    return sum;
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    MultiPostingsEnum docsEnum;

    // Can only reuse if incoming enum is also a MultiDocsEnum
    if (reuse != null && reuse instanceof MultiPostingsEnum) {
      docsEnum = (MultiPostingsEnum) reuse;
      // ... and was previously created w/ this MultiTermsEnum:
      if (!docsEnum.canReuse(this)) {
        docsEnum = new MultiPostingsEnum(this, subs.length);
      }
    } else {
      docsEnum = new MultiPostingsEnum(this, subs.length);
    }

    int upto = 0;

    ArrayUtil.timSort(top, 0, numTop, INDEX_COMPARATOR);

    for(int i=0;i<numTop;i++) {

      final TermsEnumWithSlice entry = top[i];

      assert entry.index < docsEnum.subPostingsEnums.length: entry.index + " vs " + docsEnum.subPostingsEnums.length + "; " + subs.length;
      final PostingsEnum subPostingsEnum = entry.terms.postings(docsEnum.subPostingsEnums[entry.index], flags);
      assert subPostingsEnum != null;
      docsEnum.subPostingsEnums[entry.index] = subPostingsEnum;
      subDocs[upto].postingsEnum = subPostingsEnum;
      subDocs[upto].slice = entry.subSlice;
      upto++;
    }
    
    return docsEnum.reset(subDocs, upto);
  }

  final static class TermsEnumWithSlice {
    private final ReaderSlice subSlice;
    TermsEnum terms;
    public BytesRef current;
    final int index;

    public TermsEnumWithSlice(int index, ReaderSlice subSlice) {
      this.subSlice = subSlice;
      this.index = index;
      assert subSlice.length >= 0: "length=" + subSlice.length;
    }

    public void reset(TermsEnum terms, BytesRef term) {
      this.terms = terms;
      current = term;
    }

    @Override
    public String toString() {
      return subSlice.toString()+":"+terms;
    }
  }

  private final static class TermMergeQueue extends PriorityQueue<TermsEnumWithSlice> {

    final int[] stack;

    TermMergeQueue(int size) {
      super(size);
      this.stack = new int[size];
    }

    @Override
    protected boolean lessThan(TermsEnumWithSlice termsA, TermsEnumWithSlice termsB) {
      return termsA.current.compareTo(termsB.current) < 0;
    }

    /** Add the {@link #top()} slice as well as all slices that are positionned
     *  on the same term to {@code tops} and return how many of them there are. */
    int fillTop(TermsEnumWithSlice[] tops) {
      final int size = size();
      if (size == 0) {
        return 0;
      }
      tops[0] = top();
      int numTop = 1;
      stack[0] = 1;
      int stackLen = 1;

      while (stackLen != 0) {
        final int index = stack[--stackLen];
        final int leftChild = index << 1;
        for (int child = leftChild, end = Math.min(size, leftChild + 1); child <= end; ++child) {
          TermsEnumWithSlice te = get(child);
          if (te.current.equals(tops[0].current)) {
            tops[numTop++] = te;
            stack[stackLen++] = child;
          }
        }
      }
      return numTop;
    }

    private TermsEnumWithSlice get(int i) {
      return (TermsEnumWithSlice) getHeapArray()[i];
    }
  }

  @Override
  public String toString() {
    return "MultiTermsEnum(" + Arrays.toString(subs) + ")";
  }
}
