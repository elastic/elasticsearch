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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.util.base.Preconditions.*;

/** An ordering that uses the natural order of the values. */
@GwtCompatible(serializable = true)
@SuppressWarnings("unchecked") // TODO: the right way to explain this??
final class NaturalOrdering
    extends Ordering<Comparable> implements Serializable {
  static final NaturalOrdering INSTANCE = new NaturalOrdering();

  public int compare(Comparable left, Comparable right) {
    checkNotNull(right); // left null is caught later
    if (left == right) {
      return 0;
    }

    @SuppressWarnings("unchecked") // we're permitted to throw CCE
    int result = left.compareTo(right);
    return result;
  }

  @SuppressWarnings("unchecked") // TODO: the right way to explain this??
  @Override public <S extends Comparable> Ordering<S> reverse() {
    return (Ordering) ReverseNaturalOrdering.INSTANCE;
  }

  // Override to remove a level of indirection from inner loop
  @SuppressWarnings("unchecked") // TODO: the right way to explain this??
  @Override public int binarySearch(
      List<? extends Comparable> sortedList, Comparable key) {
    return Collections.binarySearch((List) sortedList, key);
  }

  // Override to remove a level of indirection from inner loop
  @Override public <E extends Comparable> List<E> sortedCopy(
      Iterable<E> iterable) {
    List<E> list = Lists.newArrayList(iterable);
    Collections.sort(list);
    return list;
  }

  // preserving singleton-ness gives equals()/hashCode() for free
  private Object readResolve() {
    return INSTANCE;
  }

  @Override public String toString() {
    return "Ordering.natural()";
  }

  private NaturalOrdering() {}

  private static final long serialVersionUID = 0;
}
