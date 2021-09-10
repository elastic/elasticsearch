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


import java.util.AbstractCollection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link Multiset} is a set that allows for duplicate elements. Two
 * {@link Multiset}s are equal if they contain the same unique elements and if
 * each unique element has as many occurrences in both multisets.
 * Iteration order is not specified.
 * @lucene.internal
 */
final class Multiset<T> extends AbstractCollection<T> {

  private final Map<T, Integer> map = new HashMap<>();
  private int size;

  /** Create an empty {@link Multiset}. */
  Multiset() {
    super();
  }

  @Override
  public Iterator<T> iterator() {
    final Iterator<Map.Entry<T, Integer>> mapIterator = map.entrySet().iterator();
    return new Iterator<T>() {

      T current;
      int remaining;

      @Override
      public boolean hasNext() {
        return remaining > 0 || mapIterator.hasNext();
      }

      @Override
      public T next() {
        if (remaining == 0) {
          Map.Entry<T, Integer> next = mapIterator.next();
          current = next.getKey();
          remaining = next.getValue();
        }
        assert remaining > 0;
        remaining -= 1;
        return current;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void clear() {
    map.clear();
    size = 0;
  }

  @Override
  public boolean add(T e) {
    Integer currentFreq = map.get(e);
    if (currentFreq == null) {
      map.put(e, 1);
    } else {
      map.put(e, map.get(e) + 1);
    }
    size += 1;
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    final Integer count = map.get(o);
    if (count == null) {
      return false;
    } else if (1 == count.intValue()) {
      map.remove(o);
    } else {
      map.put((T) o, count - 1);
    }
    size -= 1;
    return true;
  }

  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    Multiset<?> that = (Multiset<?>) obj;
    return size == that.size // not necessary but helps escaping early
        && map.equals(that.map);
  }

  @Override
  public int hashCode() {
    return 31 * getClass().hashCode() + map.hashCode();
  }

}
