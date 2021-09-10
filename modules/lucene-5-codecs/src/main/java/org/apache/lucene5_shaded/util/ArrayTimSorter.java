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

/**
 * A {@link TimSorter} for object arrays.
 * @lucene.internal
 */
final class ArrayTimSorter<T> extends TimSorter {

  private final Comparator<? super T> comparator;
  private final T[] arr;
  private final T[] tmp;

  /** Create a new {@link ArrayTimSorter}. */
  public ArrayTimSorter(T[] arr, Comparator<? super T> comparator, int maxTempSlots) {
    super(maxTempSlots);
    this.arr = arr;
    this.comparator = comparator;
    if (maxTempSlots > 0) {
      @SuppressWarnings("unchecked")
      final T[] tmp = (T[]) new Object[maxTempSlots];
      this.tmp = tmp;
    } else {
      this.tmp = null;
    }
  }

  @Override
  protected int compare(int i, int j) {
    return comparator.compare(arr[i], arr[j]);
  }

  @Override
  protected void swap(int i, int j) {
    ArrayUtil.swap(arr, i, j);
  }

  @Override
  protected void copy(int src, int dest) {
    arr[dest] = arr[src];
  }

  @Override
  protected void save(int start, int len) {
    System.arraycopy(arr, start, tmp, 0, len);
  }

  @Override
  protected void restore(int src, int dest) {
    arr[dest] = tmp[src];
  }

  @Override
  protected int compareSaved(int i, int j) {
    return comparator.compare(tmp[i], arr[j]);
  }

}
