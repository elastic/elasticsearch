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

import static org.elasticsearch.util.base.Preconditions.*;

/** An ordering that uses the reverse of a given order. */
@GwtCompatible(serializable = true)
final class ReverseOrdering<T> extends Ordering<T> implements Serializable {
  final Ordering<? super T> forwardOrder;

  ReverseOrdering(Ordering<? super T> forwardOrder) {
    this.forwardOrder = checkNotNull(forwardOrder);
  }

  public int compare(T a, T b) {
    return forwardOrder.compare(b, a);
  }

  @SuppressWarnings("unchecked") // how to explain?
  @Override public <S extends T> Ordering<S> reverse() {
    return (Ordering) forwardOrder;
  }

  // Override the six min/max methods to "hoist" delegation outside loops

  @Override public <E extends T> E min(E a, E b) {
    return forwardOrder.max(a, b);
  }

  @Override public <E extends T> E min(E a, E b, E c, E... rest) {
    return forwardOrder.max(a, b, c, rest);
  }

  @Override public <E extends T> E min(Iterable<E> iterable) {
    return forwardOrder.max(iterable);
  }

  @Override public <E extends T> E max(E a, E b) {
    return forwardOrder.min(a, b);
  }

  @Override public <E extends T> E max(E a, E b, E c, E... rest) {
    return forwardOrder.min(a, b, c, rest);
  }

  @Override public <E extends T> E max(Iterable<E> iterable) {
    return forwardOrder.min(iterable);
  }

  @Override public int hashCode() {
    return -forwardOrder.hashCode();
  }

  @Override public boolean equals(@Nullable Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof ReverseOrdering) {
      ReverseOrdering<?> that = (ReverseOrdering<?>) object;
      return this.forwardOrder.equals(that.forwardOrder);
    }
    return false;
  }

  @Override public String toString() {
    return forwardOrder + ".reverse()";
  }

  private static final long serialVersionUID = 0;
}
