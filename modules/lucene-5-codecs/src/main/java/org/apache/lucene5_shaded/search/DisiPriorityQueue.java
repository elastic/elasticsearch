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


import java.util.Arrays;
import java.util.Iterator;

import org.apache.lucene5_shaded.util.PriorityQueue;

/**
 * A priority queue of DocIdSetIterators that orders by current doc ID.
 * This specialization is needed over {@link PriorityQueue} because the
 * pluggable comparison function makes the rebalancing quite slow.
 * @lucene.internal
 */
public final class DisiPriorityQueue implements Iterable<DisiWrapper> {

  static int leftNode(int node) {
    return ((node + 1) << 1) - 1;
  }

  static int rightNode(int leftNode) {
    return leftNode + 1;
  }

  static int parentNode(int node) {
    return ((node + 1) >>> 1) - 1;
  }

  private final DisiWrapper[] heap;
  private int size;

  public DisiPriorityQueue(int maxSize) {
    heap = new DisiWrapper[maxSize];
    size = 0;
  }

  public int size() {
    return size;
  }

  public DisiWrapper top() {
    return heap[0];
  }

  /** Get the list of scorers which are on the current doc. */
  public DisiWrapper topList() {
    final DisiWrapper[] heap = this.heap;
    final int size = this.size;
    DisiWrapper list = heap[0];
    list.next = null;
    if (size >= 3) {
      list = topList(list, heap, size, 1);
      list = topList(list, heap, size, 2);
    } else if (size == 2 && heap[1].doc == list.doc) {
      list = prepend(heap[1], list);
    }
    return list;
  }

  // prepend w1 (iterator) to w2 (list)
  private DisiWrapper prepend(DisiWrapper w1, DisiWrapper w2) {
    w1.next = w2;
    return w1;
  }

  private DisiWrapper topList(DisiWrapper list, DisiWrapper[] heap,
                                    int size, int i) {
    final DisiWrapper w = heap[i];
    if (w.doc == list.doc) {
      list = prepend(w, list);
      final int left = leftNode(i);
      final int right = left + 1;
      if (right < size) {
        list = topList(list, heap, size, left);
        list = topList(list, heap, size, right);
      } else if (left < size && heap[left].doc == list.doc) {
        list = prepend(heap[left], list);
      }
    }
    return list;
  }

  public DisiWrapper add(DisiWrapper entry) {
    final DisiWrapper[] heap = this.heap;
    final int size = this.size;
    heap[size] = entry;
    upHeap(size);
    this.size = size + 1;
    return heap[0];
  }

  public DisiWrapper pop() {
    final DisiWrapper[] heap = this.heap;
    final DisiWrapper result = heap[0];
    final int i = --size;
    heap[0] = heap[i];
    heap[i] = null;
    downHeap(i);
    return result;
  }

  public DisiWrapper updateTop() {
    downHeap(size);
    return heap[0];
  }

  DisiWrapper updateTop(DisiWrapper topReplacement) {
    heap[0] = topReplacement;
    return updateTop();
  }

  void upHeap(int i) {
    final DisiWrapper node = heap[i];
    final int nodeDoc = node.doc;
    int j = parentNode(i);
    while (j >= 0 && nodeDoc < heap[j].doc) {
      heap[i] = heap[j];
      i = j;
      j = parentNode(j);
    }
    heap[i] = node;
  }

  void downHeap(int size) {
    int i = 0;
    final DisiWrapper node = heap[0];
    int j = leftNode(i);
    if (j < size) {
      int k = rightNode(j);
      if (k < size && heap[k].doc < heap[j].doc) {
        j = k;
      }
      if (heap[j].doc < node.doc) {
        do {
          heap[i] = heap[j];
          i = j;
          j = leftNode(i);
          k = rightNode(j);
          if (k < size && heap[k].doc < heap[j].doc) {
            j = k;
          }
        } while (j < size && heap[j].doc < node.doc);
        heap[i] = node;
      }
    }
  }

  @Override
  public Iterator<DisiWrapper> iterator() {
    return Arrays.asList(heap).subList(0, size).iterator();
  }

}


