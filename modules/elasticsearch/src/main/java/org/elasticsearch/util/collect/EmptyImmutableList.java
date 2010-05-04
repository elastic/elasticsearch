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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * An empty immutable list.
 * 
 * @author Kevin Bourrillion
 */
@GwtCompatible(serializable = true)
final class EmptyImmutableList extends ImmutableList<Object> {
  static final EmptyImmutableList INSTANCE = new EmptyImmutableList();

  private EmptyImmutableList() {}

  public int size() {
    return 0;
  }

  @Override public boolean isEmpty() {
    return true;
  }

  @Override public boolean contains(Object target) {
    return false;
  }

  @Override public UnmodifiableIterator<Object> iterator() {
    return Iterators.emptyIterator();
  }

  private static final Object[] EMPTY_ARRAY = new Object[0];

  @Override public Object[] toArray() {
    return EMPTY_ARRAY;
  }

  @Override public <T> T[] toArray(T[] a) {
    if (a.length > 0) {
      a[0] = null;
    }
    return a;
  }

  public Object get(int index) {
    // guaranteed to fail, but at least we get a consistent message
    checkElementIndex(index, 0);
    throw new AssertionError("unreachable");
  }

  @Override public int indexOf(Object target) {
    return -1;
  }

  @Override public int lastIndexOf(Object target) {
    return -1;
  }

  @Override public ImmutableList<Object> subList(int fromIndex, int toIndex) {
    checkPositionIndexes(fromIndex, toIndex, 0);
    return this;
  }

  public ListIterator<Object> listIterator() {
    return Collections.emptyList().listIterator();
  }

  public ListIterator<Object> listIterator(int start) {
    checkPositionIndex(start, 0);
    return Collections.emptyList().listIterator();
  }

  @Override public boolean containsAll(Collection<?> targets) {
    return targets.isEmpty();
  }

  @Override public boolean equals(@Nullable Object object) {
    if (object instanceof List) {
      List<?> that = (List<?>) object;
      return that.isEmpty();
    }
    return false;
  }

  @Override public int hashCode() {
    return 1;
  }

  @Override public String toString() {
    return "[]";
  }

  Object readResolve() {
    return INSTANCE; // preserve singleton property
  }

  private static final long serialVersionUID = 0;
}
