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



import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.RandomAccess;

/**
 * Methods for manipulating (sorting) collections.
 * Sort methods work directly on the supplied lists and don't copy to/from arrays
 * before/after. For medium size collections as used in the Lucene indexer that is
 * much more efficient.
 *
 * @lucene.internal
 */

public final class CollectionUtil {

  private CollectionUtil() {} // no instance
  private static final class ListIntroSorter<T> extends IntroSorter {

    T pivot;
    final List<T> list;
    final Comparator<? super T> comp;

    ListIntroSorter(List<T> list, Comparator<? super T> comp) {
      super();
      if (!(list instanceof RandomAccess))
        throw new IllegalArgumentException("CollectionUtil can only sort random access lists in-place.");
      this.list = list;
      this.comp = comp;
    }

    @Override
    protected void setPivot(int i) {
      pivot = list.get(i);
    }

    @Override
    protected void swap(int i, int j) {
      Collections.swap(list, i, j);
    }

    @Override
    protected int compare(int i, int j) {
      return comp.compare(list.get(i), list.get(j));
    }

    @Override
    protected int comparePivot(int j) {
      return comp.compare(pivot, list.get(j));
    }

  }

  private static final class ListTimSorter<T> extends TimSorter {

    final List<T> list;
    final Comparator<? super T> comp;
    final T[] tmp;

    @SuppressWarnings("unchecked")
    ListTimSorter(List<T> list, Comparator<? super T> comp, int maxTempSlots) {
      super(maxTempSlots);
      if (!(list instanceof RandomAccess))
        throw new IllegalArgumentException("CollectionUtil can only sort random access lists in-place.");
      this.list = list;
      this.comp = comp;
      if (maxTempSlots > 0) {
        this.tmp = (T[]) new Object[maxTempSlots];
      } else {
        this.tmp = null;
      }
    }

    @Override
    protected void swap(int i, int j) {
      Collections.swap(list, i, j);
    }

    @Override
    protected void copy(int src, int dest) {
      list.set(dest, list.get(src));
    }

    @Override
    protected void save(int i, int len) {
      for (int j = 0; j < len; ++j) {
        tmp[j] = list.get(i + j);
      }
    }

    @Override
    protected void restore(int i, int j) {
      list.set(j, tmp[i]);
    }

    @Override
    protected int compare(int i, int j) {
      return comp.compare(list.get(i), list.get(j));
    }

    @Override
    protected int compareSaved(int i, int j) {
      return comp.compare(tmp[i], list.get(j));
    }

  }

  /**
   * Sorts the given random access {@link List} using the {@link Comparator}.
   * The list must implement {@link RandomAccess}. This method uses the intro sort
   * algorithm, but falls back to insertion sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T> void introSort(List<T> list, Comparator<? super T> comp) {
    final int size = list.size();
    if (size <= 1) return;
    new ListIntroSorter<>(list, comp).sort(0, size);
  }
  
  /**
   * Sorts the given random access {@link List} in natural order.
   * The list must implement {@link RandomAccess}. This method uses the intro sort
   * algorithm, but falls back to insertion sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T extends Comparable<? super T>> void introSort(List<T> list) {
    final int size = list.size();
    if (size <= 1) return;
    introSort(list, ArrayUtil.<T>naturalComparator());
  }

  // Tim sorts:
  
  /**
   * Sorts the given random access {@link List} using the {@link Comparator}.
   * The list must implement {@link RandomAccess}. This method uses the Tim sort
   * algorithm, but falls back to binary sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T> void timSort(List<T> list, Comparator<? super T> comp) {
    final int size = list.size();
    if (size <= 1) return;
    new ListTimSorter<>(list, comp, list.size() / 64).sort(0, size);
  }
  
  /**
   * Sorts the given random access {@link List} in natural order.
   * The list must implement {@link RandomAccess}. This method uses the Tim sort
   * algorithm, but falls back to binary sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T extends Comparable<? super T>> void timSort(List<T> list) {
    final int size = list.size();
    if (size <= 1) return;
    timSort(list, ArrayUtil.<T>naturalComparator());
  }

}
