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

import static org.elasticsearch.util.base.Preconditions.*;

/** An ordering that uses the reverse of the natural order of the values. */
@GwtCompatible(serializable = true)
@SuppressWarnings("unchecked") // TODO: the right way to explain this??
final class ReverseNaturalOrdering
    extends Ordering<Comparable> implements Serializable {
  static final ReverseNaturalOrdering INSTANCE = new ReverseNaturalOrdering();

  public int compare(Comparable left, Comparable right) {
    checkNotNull(left); // right null is caught later
    if (left == right) {
      return 0;
    }

    @SuppressWarnings("unchecked") // we're permitted to throw CCE
    int result = right.compareTo(left);
    return result;
  }

  @Override public <S extends Comparable> Ordering<S> reverse() {
    return Ordering.natural();
  }

  // Override the six min/max methods to "hoist" delegation outside loops

  @Override public <E extends Comparable> E min(E a, E b) {
    return NaturalOrdering.INSTANCE.max(a, b);
  }

  @Override public <E extends Comparable> E min(E a, E b, E c, E... rest) {
    return NaturalOrdering.INSTANCE.max(a, b, c, rest);
  }

  @Override public <E extends Comparable> E min(Iterable<E> iterable) {
    return NaturalOrdering.INSTANCE.max(iterable);
  }

  @Override public <E extends Comparable> E max(E a, E b) {
    return NaturalOrdering.INSTANCE.min(a, b);
  }

  @Override public <E extends Comparable> E max(E a, E b, E c, E... rest) {
    return NaturalOrdering.INSTANCE.min(a, b, c, rest);
  }

  @Override public <E extends Comparable> E max(Iterable<E> iterable) {
    return NaturalOrdering.INSTANCE.min(iterable);
  }
  
  // preserving singleton-ness gives equals()/hashCode() for free
  private Object readResolve() {
    return INSTANCE;
  }

  @Override public String toString() {
    return "Ordering.natural().reverse()";
  }

  private ReverseNaturalOrdering() {}

  private static final long serialVersionUID = 0;
}
