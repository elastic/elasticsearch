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


import java.util.Comparator;

/** Base class for sorting algorithms implementations.
 * @lucene.internal */
public abstract class Sorter {

  static final int THRESHOLD = 20;

  /** Sole constructor, used for inheritance. */
  protected Sorter() {}

  /** Compare entries found in slots <code>i</code> and <code>j</code>.
   *  The contract for the returned value is the same as
   *  {@link Comparator#compare(Object, Object)}. */
  protected abstract int compare(int i, int j);

  /** Swap values at slots <code>i</code> and <code>j</code>. */
  protected abstract void swap(int i, int j);

  /** Sort the slice which starts at <code>from</code> (inclusive) and ends at
   *  <code>to</code> (exclusive). */
  public abstract void sort(int from, int to);

  void checkRange(int from, int to) {
    if (to < from) {
      throw new IllegalArgumentException("'to' must be >= 'from', got from=" + from + " and to=" + to);
    }
  }

  void mergeInPlace(int from, int mid, int to) {
    if (from == mid || mid == to || compare(mid - 1, mid) <= 0) {
      return;
    } else if (to - from == 2) {
      swap(mid - 1, mid);
      return;
    }
    while (compare(from, mid) <= 0) {
      ++from;
    }
    while (compare(mid - 1, to - 1) <= 0) {
      --to;
    }
    int first_cut, second_cut;
    int len11, len22;
    if (mid - from > to - mid) {
      len11 = (mid - from) >>> 1;
      first_cut = from + len11;
      second_cut = lower(mid, to, first_cut);
      len22 = second_cut - mid;
    } else {
      len22 = (to - mid) >>> 1;
      second_cut = mid + len22;
      first_cut = upper(from, mid, second_cut);
      len11 = first_cut - from;
    }
    rotate(first_cut, mid, second_cut);
    final int new_mid = first_cut + len22;
    mergeInPlace(from, first_cut, new_mid);
    mergeInPlace(new_mid, second_cut, to);
  }

  int lower(int from, int to, int val) {
    int len = to - from;
    while (len > 0) {
      final int half = len >>> 1;
      final int mid = from + half;
      if (compare(mid, val) < 0) {
        from = mid + 1;
        len = len - half -1;
      } else {
        len = half;
      }
    }
    return from;
  }

  int upper(int from, int to, int val) {
    int len = to - from;
    while (len > 0) {
      final int half = len >>> 1;
      final int mid = from + half;
      if (compare(val, mid) < 0) {
        len = half;
      } else {
        from = mid + 1;
        len = len - half -1;
      }
    }
    return from;
  }

  // faster than lower when val is at the end of [from:to[
  int lower2(int from, int to, int val) {
    int f = to - 1, t = to;
    while (f > from) {
      if (compare(f, val) < 0) {
        return lower(f, t, val);
      }
      final int delta = t - f;
      t = f;
      f -= delta << 1;
    }
    return lower(from, t, val);
  }

  // faster than upper when val is at the beginning of [from:to[
  int upper2(int from, int to, int val) {
    int f = from, t = f + 1;
    while (t < to) {
      if (compare(t, val) > 0) {
        return upper(f, t, val);
      }
      final int delta = t - f;
      f = t;
      t += delta << 1;
    }
    return upper(f, to, val);
  }

  final void reverse(int from, int to) {
    for (--to; from < to; ++from, --to) {
      swap(from, to);
    }
  }

  final void rotate(int lo, int mid, int hi) {
    assert lo <= mid && mid <= hi;
    if (lo == mid || mid == hi) {
      return;
    }
    doRotate(lo, mid, hi);
  }

  void doRotate(int lo, int mid, int hi) {
    if (mid - lo == hi - mid) {
      // happens rarely but saves n/2 swaps
      while (mid < hi) {
        swap(lo++, mid++);
      }
    } else {
      reverse(lo, mid);
      reverse(mid, hi);
      reverse(lo, hi);
    }
  }

  void insertionSort(int from, int to) {
    for (int i = from + 1; i < to; ++i) {
      for (int j = i; j > from; --j) {
        if (compare(j - 1, j) > 0) {
          swap(j - 1, j);
        } else {
          break;
        }
      }
    }
  }

  void binarySort(int from, int to) {
    binarySort(from, to, from + 1);
  }

  void binarySort(int from, int to, int i) {
    for ( ; i < to; ++i) {
      int l = from;
      int h = i - 1;
      while (l <= h) {
        final int mid = (l + h) >>> 1;
        final int cmp = compare(i, mid);
        if (cmp < 0) {
          h = mid - 1;
        } else {
          l = mid + 1;
        }
      }
      switch (i - l) {
      case 2:
        swap(l + 1, l + 2);
        swap(l, l + 1);
        break;
      case 1:
        swap(l, l + 1);
        break;
      case 0:
        break;
      default:
        for (int j = i; j > l; --j) {
          swap(j - 1, j);
        }
        break;
      }
    }
  }

  void heapSort(int from, int to) {
    if (to - from <= 1) {
      return;
    }
    heapify(from, to);
    for (int end = to - 1; end > from; --end) {
      swap(from, end);
      siftDown(from, from, end);
    }
  }

  void heapify(int from, int to) {
    for (int i = heapParent(from, to - 1); i >= from; --i) {
      siftDown(i, from, to);
    }
  }

  void siftDown(int i, int from, int to) {
    for (int leftChild = heapChild(from, i); leftChild < to; leftChild = heapChild(from, i)) {
      final int rightChild = leftChild + 1;
      if (compare(i, leftChild) < 0) {
        if (rightChild < to && compare(leftChild, rightChild) < 0) {
          swap(i, rightChild);
          i = rightChild;
        } else {
          swap(i, leftChild);
          i = leftChild;
        }
      } else if (rightChild < to && compare(i, rightChild) < 0) {
        swap(i, rightChild);
        i = rightChild;
      } else {
        break;
      }
    }
  }

  static int heapParent(int from, int i) {
    return ((i - 1 - from) >>> 1) + from;
  }

  static int heapChild(int from, int i) {
    return ((i - from) << 1) + 1 + from;
  }

}
