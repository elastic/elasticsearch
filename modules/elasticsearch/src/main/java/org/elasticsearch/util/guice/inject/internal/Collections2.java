/*
 * Copyright (C) 2008 Google Inc.
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

package org.elasticsearch.util.guice.inject.internal;

import org.elasticsearch.util.gcommon.collect.Lists;

import java.util.Collection;
import java.util.Set;

/**
 * Provides static methods for working with {@code Collection} instances.
 *
 * @author Chris Povirk
 * @author Mike Bostock
 * @author Jared Levy
 */
public final class Collections2 {
  private Collections2() {}

  /**
   * Converts an iterable into a collection. If the iterable is already a
   * collection, it is returned. Otherwise, an {@link java.util.ArrayList} is
   * created with the contents of the iterable in same iteration order.
   */
  static <E> Collection<E> toCollection(Iterable<E> iterable) {
    return (iterable instanceof Collection)
        ? (Collection<E>) iterable : Lists.newArrayList(iterable);
  }

  static boolean setEquals(Set<?> thisSet, @Nullable Object object) {
    if (object == thisSet) {
      return true;
    }
    if (object instanceof Set) {
      Set<?> thatSet = (Set<?>) object;
      return thisSet.size() == thatSet.size()
          && thisSet.containsAll(thatSet);
    }
    return false;
  }
}
