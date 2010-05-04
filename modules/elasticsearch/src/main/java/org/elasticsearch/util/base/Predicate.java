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

package org.elasticsearch.util.base;

import org.elasticsearch.util.annotations.GwtCompatible;

import javax.annotation.Nullable;

/**
 * Determines a true or false value for a given input. For example, a
 * {@code RegexPredicate} might implement {@code Predicate<String>}, and return
 * {@code true} for any string that matches its given regular expression.
 *
 * <p>Implementations which may cause side effects upon evaluation are strongly
 * encouraged to state this fact clearly in their API documentation.
 *
 * @author Kevin Bourrillion
 */
@GwtCompatible
public interface Predicate<T> {

  /*
   * This interface does not extend Function<T, Boolean> because doing so would
   * let predicates return null.
   */

  /**
   * Applies this predicate to the given object.
   *
   * @param input the input that the predicate should act on
   * @return the value of this predicate when applied to the input {@code t}
   */
  boolean apply(@Nullable T input);

  /**
   * Indicates whether some other object is equal to this {@code Predicate}.
   * This method can return {@code true} <i>only</i> if the specified object is
   * also a {@code Predicate} and, for every input object {@code input}, it
   * returns exactly the same value. Thus, {@code predicate1.equals(predicate2)}
   * implies that either {@code predicate1.apply(input)} and
   * {@code predicate2.apply(input)} are both {@code true} or both
   * {@code false}.
   *
   * <p>Note that it is always safe <i>not</i> to override
   * {@link Object#equals}.
   */
  boolean equals(@Nullable Object obj);
}
