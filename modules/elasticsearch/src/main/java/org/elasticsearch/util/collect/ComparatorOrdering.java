/*
 * Copyright (C) 2007 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.util.collect;

import org.elasticsearch.util.annotations.GwtCompatible;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.util.base.Preconditions.*;

/** An ordering for a pre-existing {@code comparator}. */
@GwtCompatible(serializable = true)
final class ComparatorOrdering<T> extends Ordering<T> implements Serializable {
  final Comparator<T> comparator;

  ComparatorOrdering(Comparator<T> comparator) {
    this.comparator = checkNotNull(comparator);
  }

  public int compare(T a, T b) {
    return comparator.compare(a, b);
  }

  // Override just to remove a level of indirection from inner loops
  @Override public int binarySearch(List<? extends T> sortedList, T key) {
    return Collections.binarySearch(sortedList, key, comparator);
  }

  // Override just to remove a level of indirection from inner loops
  @Override public <E extends T> List<E> sortedCopy(Iterable<E> iterable) {
    List<E> list = Lists.newArrayList(iterable);
    Collections.sort(list, comparator);
    return list;
  }

  @Override public boolean equals(@Nullable Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof ComparatorOrdering) {
      ComparatorOrdering<?> that = (ComparatorOrdering<?>) object;
      return this.comparator.equals(that.comparator);
    }
    return false;
  }

  @Override public int hashCode() {
    return comparator.hashCode();
  }

  @Override public String toString() {
    return comparator.toString();
  }

  private static final long serialVersionUID = 0;
}
