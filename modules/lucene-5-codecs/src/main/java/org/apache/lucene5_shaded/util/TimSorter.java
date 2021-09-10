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
package org.apache.lucene5_shaded.util;


import java.util.Arrays;

/**
 * {@link Sorter} implementation based on the
 * <a href="http://svn.python.org/projects/python/trunk/Objects/listsort.txt">TimSort</a>
 * algorithm.
 * <p>This implementation is especially good at sorting partially-sorted
 * arrays and sorts small arrays with binary sort.
 * <p><b>NOTE</b>:There are a few differences with the original implementation:<ul>
 * <li><a name="maxTempSlots"></a>The extra amount of memory to perform merges is
 * configurable. This allows small merges to be very fast while large merges
 * will be performed in-place (slightly slower). You can make sure that the
 * fast merge routine will always be used by having <code>maxTempSlots</code>
 * equal to half of the length of the slice of data to sort.
 * <li>Only the fast merge routine can gallop (the one that doesn't run
 * in-place) and it only gallops on the longest slice.
 * </ul>
 * @lucene.internal
 */
public abstract class TimSorter extends Sorter {

  static final int MINRUN = 32;
  static final int THRESHOLD = 64;
  static final int STACKSIZE = 49; // depends on MINRUN
  static final int MIN_GALLOP = 7;

  final int maxTempSlots;
  int minRun;
  int to;
  int stackSize;
  int[] runEnds;

  /**
   * Create a new {@link TimSorter}.
   * @param maxTempSlots the <a href="#maxTempSlots">maximum amount of extra memory to run merges</a>
   */
  protected TimSorter(int maxTempSlots) {
    super();
    runEnds = new int[1 + STACKSIZE];
    this.maxTempSlots = maxTempSlots;
  }

  /** Minimum run length for an array of length <code>length</code>. */
  static int minRun(int length) {
    assert length >= MINRUN;
    int n = length;
    int r = 0;
    while (n >= 64) {
      r |= n & 1;
      n >>>= 1;
    }
    final int minRun = n + r;
    assert minRun >= MINRUN && minRun <= THRESHOLD;
    return minRun;
  }

  int runLen(int i) {
    final int off = stackSize - i;
    return runEnds[off] - runEnds[off - 1];
  }

  int runBase(int i) {
    return runEnds[stackSize - i - 1];
  }

  int runEnd(int i) {
    return runEnds[stackSize - i];
  }

  void setRunEnd(int i, int runEnd) {
    runEnds[stackSize - i] = runEnd;
  }

  void pushRunLen(int len) {
    runEnds[stackSize + 1] = runEnds[stackSize] + len;
    ++stackSize;
  }

  /** Compute the length of the next run, make the run sorted and return its
   *  length. */
  int nextRun() {
    final int runBase = runEnd(0);
    assert runBase < to;
    if (runBase == to - 1) {
      return 1;
    }
    int o = runBase + 2;
    if (compare(runBase, runBase+1) > 0) {
      // run must be strictly descending
      while (o < to && compare(o - 1, o) > 0) {
        ++o;
      }
      reverse(runBase, o);
    } else {
      // run must be non-descending
      while (o < to && compare(o - 1, o) <= 0) {
        ++o;
      }
    }
    final int runHi = Math.max(o, Math.min(to, runBase + minRun));
    binarySort(runBase, runHi, o);
    return runHi - runBase;
  }

  void ensureInvariants() {
    while (stackSize > 1) {
      final int runLen0 = runLen(0);
      final int runLen1 = runLen(1);

      if (stackSize > 2) {
        final int runLen2 = runLen(2);

        if (runLen2 <= runLen1 + runLen0) {
          // merge the smaller of 0 and 2 with 1
          if (runLen2 < runLen0) {
            mergeAt(1);
          } else {
            mergeAt(0);
          }
          continue;
        }
      }

      if (runLen1 <= runLen0) {
        mergeAt(0);
        continue;
      }

      break;
    }
  }

  void exhaustStack() {
    while (stackSize > 1) {
      mergeAt(0);
    }
  }

  void reset(int from, int to) {
    stackSize = 0;
    Arrays.fill(runEnds, 0);
    runEnds[0] = from;
    this.to = to;
    final int length = to - from;
    this.minRun = length <= THRESHOLD ? length : minRun(length);
  }

  void mergeAt(int n) {
    assert stackSize >= 2;
    merge(runBase(n + 1), runBase(n), runEnd(n));
    for (int j = n + 1; j > 0; --j) {
      setRunEnd(j, runEnd(j-1));
    }
    --stackSize;
  }

  void merge(int lo, int mid, int hi) {
    if (compare(mid - 1, mid) <= 0) {
      return;
    }
    lo = upper2(lo, mid, mid);
    hi = lower2(mid, hi, mid - 1);

    if (hi - mid <= mid - lo && hi - mid <= maxTempSlots) {
      mergeHi(lo, mid, hi);
    } else if (mid - lo <= maxTempSlots) {
      mergeLo(lo, mid, hi);
    } else {
      mergeInPlace(lo, mid, hi);
    }
  }

  @Override
  public void sort(int from, int to) {
    checkRange(from, to);
    if (to - from <= 1) {
      return;
    }
    reset(from, to);
    do {
      ensureInvariants();
      pushRunLen(nextRun());
    } while (runEnd(0) < to);
    exhaustStack();
    assert runEnd(0) == to;
  }

  @Override
  void doRotate(int lo, int mid, int hi) {
    final int len1 = mid - lo;
    final int len2 = hi - mid;
    if (len1 == len2) {
      while (mid < hi) {
        swap(lo++, mid++);
      }
    } else if (len2 < len1 && len2 <= maxTempSlots) {
      save(mid, len2);
      for (int i = lo + len1 - 1, j = hi - 1; i >= lo; --i, --j) {
        copy(i, j);
      }
      for (int i = 0, j = lo; i < len2; ++i, ++j) {
        restore(i, j);
      }
    } else if (len1 <= maxTempSlots) {
      save(lo, len1);
      for (int i = mid, j = lo; i < hi; ++i, ++j) {
        copy(i, j);
      }
      for (int i = 0, j = lo + len2; j < hi; ++i, ++j) {
        restore(i, j);
      }
    } else {
      reverse(lo, mid);
      reverse(mid, hi);
      reverse(lo, hi);
    }
  }

  void mergeLo(int lo, int mid, int hi) {
    assert compare(lo, mid) > 0;
    int len1 = mid - lo;
    save(lo, len1);
    copy(mid, lo);
    int i = 0, j = mid + 1, dest = lo + 1;
    outer: for (;;) {
      for (int count = 0; count < MIN_GALLOP; ) {
        if (i >= len1 || j >= hi) {
          break outer;
        } else if (compareSaved(i, j) <= 0) {
          restore(i++, dest++);
          count = 0;
        } else {
          copy(j++, dest++);
          ++count;
        }
      }
      // galloping...
      int next = lowerSaved3(j, hi, i);
      for (; j < next; ++dest) {
        copy(j++, dest);
      }
      restore(i++, dest++);
    }
    for (; i < len1; ++dest) {
      restore(i++, dest);
    }
    assert j == dest;
  }

  void mergeHi(int lo, int mid, int hi) {
    assert compare(mid - 1, hi - 1) > 0;
    int len2 = hi - mid;
    save(mid, len2);
    copy(mid - 1, hi - 1);
    int i = mid - 2, j = len2 - 1, dest = hi - 2;
    outer: for (;;) {
      for (int count = 0; count < MIN_GALLOP; ) {
        if (i < lo || j < 0) {
          break outer;
        } else if (compareSaved(j, i) >= 0) {
          restore(j--, dest--);
          count = 0;
        } else {
          copy(i--, dest--);
          ++count;
        }
      }
      // galloping
      int next = upperSaved3(lo, i + 1, j);
      while (i >= next) {
        copy(i--, dest--);
      }
      restore(j--, dest--);
    }
    for (; j >= 0; --dest) {
      restore(j--, dest);
    }
    assert i == dest;
  }

  int lowerSaved(int from, int to, int val) {
    int len = to - from;
    while (len > 0) {
      final int half = len >>> 1;
      final int mid = from + half;
      if (compareSaved(val, mid) > 0) {
        from = mid + 1;
        len = len - half -1;
      } else {
        len = half;
      }
    }
    return from;
  }

  int upperSaved(int from, int to, int val) {
    int len = to - from;
    while (len > 0) {
      final int half = len >>> 1;
      final int mid = from + half;
      if (compareSaved(val, mid) < 0) {
        len = half;
      } else {
        from = mid + 1;
        len = len - half -1;
      }
    }
    return from;
  }

  // faster than lowerSaved when val is at the beginning of [from:to[
  int lowerSaved3(int from, int to, int val) {
    int f = from, t = f + 1;
    while (t < to) {
      if (compareSaved(val, t) <= 0) {
        return lowerSaved(f, t, val);
      }
      int delta = t - f;
      f = t;
      t += delta << 1;
    }
    return lowerSaved(f, to, val);
  }

  //faster than upperSaved when val is at the end of [from:to[
  int upperSaved3(int from, int to, int val) {
    int f = to - 1, t = to;
    while (f > from) {
      if (compareSaved(val, f) >= 0) {
        return upperSaved(f, t, val);
      }
      final int delta = t - f;
      t = f;
      f -= delta << 1;
    }
    return upperSaved(from, t, val);
  }

  /** Copy data from slot <code>src</code> to slot <code>dest</code>. */
  protected abstract void copy(int src, int dest);

  /** Save all elements between slots <code>i</code> and <code>i+len</code>
   *  into the temporary storage. */
  protected abstract void save(int i, int len);

  /** Restore element <code>j</code> from the temporary storage into slot <code>i</code>. */
  protected abstract void restore(int i, int j);

  /** Compare element <code>i</code> from the temporary storage with element
   *  <code>j</code> from the slice to sort, similarly to
   *  {@link #compare(int, int)}. */
  protected abstract int compareSaved(int i, int j);

}
