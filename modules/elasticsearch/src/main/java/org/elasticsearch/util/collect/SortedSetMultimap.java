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
import java.util.*;

/**
 * A {@code SetMultimap} whose set of values for a given key are kept sorted;
 * that is, they comprise a {@link SortedSet}. It cannot hold duplicate
 * key-value pairs; adding a key-value pair that's already in the multimap has
 * no effect. This interface does not specify the ordering of the multimap's
 * keys.
 *
 * <p>The {@link #get}, {@link #removeAll}, and {@link #replaceValues} methods
 * each return a {@link SortedSet} of values, while {@link Multimap#entries()}
 * returns a {@link Set} of map entries. Though the method signature doesn't say
 * so explicitly, the map returned by {@link #asMap} has {@code SortedSet}
 * values.
 *
 * @author Jared Levy
 */
@GwtCompatible
public interface SortedSetMultimap<K, V> extends SetMultimap<K, V> {
  /**
   * Returns a collection view of all values associated with a key. If no
   * mappings in the multimap have the provided key, an empty collection is
   * returned.
   *
   * <p>Changes to the returned collection will update the underlying multimap,
   * and vice versa.
   *
   * <p>Because a {@code SortedSetMultimap} has unique sorted values for a given
   * key, this method returns a {@link SortedSet}, instead of the
   * {@link java.util.Collection} specified in the {@link Multimap} interface.
   */
  SortedSet<V> get(@Nullable K key);

  /**
   * Removes all values associated with a given key.
   *
   * <p>Because a {@code SortedSetMultimap} has unique sorted values for a given
   * key, this method returns a {@link SortedSet}, instead of the
   * {@link java.util.Collection} specified in the {@link Multimap} interface.
   */
  SortedSet<V> removeAll(@Nullable Object key);

  /**
   * Stores a collection of values with the same key, replacing any existing
   * values for that key.
   *
   * <p>Because a {@code SortedSetMultimap} has unique sorted values for a given
   * key, this method returns a {@link SortedSet}, instead of the
   * {@link java.util.Collection} specified in the {@link Multimap} interface.
   *
   * <p>Any duplicates in {@code values} will be stored in the multimap once.
   */
  SortedSet<V> replaceValues(K key, Iterable<? extends V> values);

  /**
   * Returns a map view that associates each key with the corresponding values
   * in the multimap. Changes to the returned map, such as element removal,
   * will update the underlying multimap. The map never supports
   * {@code setValue()} on the map entries, {@code put}, or {@code putAll}.
   *
   * <p>The collections returned by {@code asMap().get(Object)} have the same
   * behavior as those returned by {@link #get}.
   *
   * <p>Though the method signature doesn't say so explicitly, the returned map
   * has {@link SortedSet} values.
   */
  Map<K, Collection<V>> asMap();

  /**
   * Returns the comparator that orders the multimap values, with a {@code null}
   * indicating that natural ordering is used.
   */
  Comparator<? super V> valueComparator();
}
