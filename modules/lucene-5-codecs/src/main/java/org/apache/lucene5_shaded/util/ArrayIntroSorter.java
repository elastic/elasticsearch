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
 * An {@link IntroSorter} for object arrays.
 * @lucene.internal
 */
final class ArrayIntroSorter<T> extends IntroSorter {

  private final T[] arr;
  private final Comparator<? super T> comparator;
  private T pivot;

  /** Create a new {@link ArrayInPlaceMergeSorter}. */
  public ArrayIntroSorter(T[] arr, Comparator<? super T> comparator) {
    this.arr = arr;
    this.comparator = comparator;
    pivot = null;
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
  protected void setPivot(int i) {
    pivot = arr[i];
  }

  @Override
  protected int comparePivot(int i) {
    return comparator.compare(pivot, arr[i]);
  }

}
