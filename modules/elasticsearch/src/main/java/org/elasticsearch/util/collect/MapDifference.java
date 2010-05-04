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
import java.util.Map;

/**
 * An object representing the differences between two maps.
 *
 * @author Kevin Bourrillion
 */
@GwtCompatible
public interface MapDifference<K, V> {
  /**
   * Returns {@code true} if there are no differences between the two maps;
   * that is, if the maps are equal.
   */
  boolean areEqual();

  /**
   * Returns an unmodifiable map containing the entries from the left map whose
   * keys are not present in the right map.
   */
  Map<K, V> entriesOnlyOnLeft();

  /**
   * Returns an unmodifiable map containing the entries from the right map whose
   * keys are not present in the left map.
   */
  Map<K, V> entriesOnlyOnRight();

  /**
   * Returns an unmodifiable map containing the entries that appear in both
   * maps; that is, the intersection of the two maps.
   */
  Map<K, V> entriesInCommon();

  /**
   * Returns an unmodifiable map describing keys that appear in both maps, but
   * with different values.
   */
  Map<K, ValueDifference<V>> entriesDiffering();

  /**
   * Compares the specified object with this instance for equality. Returns
   * {@code true} if the given object is also a {@code MapDifference} and the
   * values returned by the {@link #entriesOnlyOnLeft()}, {@link
   * #entriesOnlyOnRight()}, {@link #entriesInCommon()} and {@link
   * #entriesDiffering()} of the two instances are equal.
   */
  boolean equals(@Nullable Object object);

  /**
   * Returns the hash code for this instance. This is defined as the hash code
   * of <pre>   {@code
   *
   *   Arrays.asList(entriesOnlyOnLeft(), entriesOnlyOnRight(),
   *       entriesInCommon(), entriesDiffering())}</pre>
   */
  int hashCode();

  /**
   * A difference between the mappings from two maps with the same key. The
   * {@code leftValue()} and {@code rightValue} are not equal, and one but not
   * both of them may be null.
   */
  interface ValueDifference<V> {
    /**
     * Returns the value from the left map (possibly null).
     */
    V leftValue();

    /**
     * Returns the value from the right map (possibly null).
     */
    V rightValue();

    /**
     * Two instances are considered equal if their {@link #leftValue()}
     * values are equal and their {@link #rightValue()} values are also equal.
     */
    @Override boolean equals(@Nullable Object other);

    /**
     * The hash code equals the value
     * {@code Arrays.asList(leftValue(), rightValue()).hashCode()}.
     */
    @Override int hashCode();
  }

}
