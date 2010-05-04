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
import java.util.Map;
import java.util.Set;

/**
 * A collection similar to a {@code Map}, but which may associate multiple
 * values with a single key. If you call {@link #put} twice, with the same key
 * but different values, the multimap contains mappings from the key to both
 * values.
 *
 * <p>The methods {@link #get}, {@link #keySet}, {@link #keys}, {@link #values},
 * {@link #entries}, and {@link #asMap} return collections that are views of the
 * multimap. If the multimap is modifiable, updating it can change the contents
 * of those collections, and updating the collections will change the multimap.
 * In contrast, {@link #replaceValues} and {@link #removeAll} return collections
 * that are independent of subsequent multimap changes.
 *
 * <p>Depending on the implementation, a multimap may or may not allow duplicate
 * key-value pairs. In other words, the multimap contents after adding the same
 * key and value twice varies between implementations. In multimaps allowing
 * duplicates, the multimap will contain two mappings, and {@code get} will
 * return a collection that includes the value twice. In multimaps not
 * supporting duplicates, the multimap will contain a single mapping from the
 * key to the value, and {@code get} will return a collection that includes the
 * value once.
 *
 * <p>All methods that alter the multimap are optional, and the views returned
 * by the multimap may or may not be modifiable. When modification isn't
 * supported, those methods will throw an {@link UnsupportedOperationException}.
 *
 * @author Jared Levy
 * @param <K> the type of keys maintained by this multimap
 * @param <V> the type of mapped values
 */
@GwtCompatible
public interface Multimap<K, V> {
  // Query Operations

  /** Returns the number of key-value pairs in the multimap. */
  int size();

  /** Returns {@code true} if the multimap contains no key-value pairs. */
  boolean isEmpty();

  /**
   * Returns {@code true} if the multimap contains any values for the specified
   * key.
   *
   * @param key key to search for in multimap
   */
  boolean containsKey(@Nullable Object key);

  /**
   * Returns {@code true} if the multimap contains the specified value for any
   * key.
   *
   * @param value value to search for in multimap
   */
  boolean containsValue(@Nullable Object value);

  /**
   * Returns {@code true} if the multimap contains the specified key-value pair.
   *
   * @param key key to search for in multimap
   * @param value value to search for in multimap
   */
  boolean containsEntry(@Nullable Object key, @Nullable Object value);

  // Modification Operations

  /**
   * Stores a key-value pair in the multimap.
   *
   * <p>Some multimap implementations allow duplicate key-value pairs, in which
   * case {@code put} always adds a new key-value pair and increases the
   * multimap size by 1. Other implementations prohibit duplicates, and storing
   * a key-value pair that's already in the multimap has no effect.
   *
   * @param key key to store in the multimap
   * @param value value to store in the multimap
   * @return {@code true} if the method increased the size of the multimap, or
   *     {@code false} if the multimap already contained the key-value pair and
   *     doesn't allow duplicates
   */
  boolean put(@Nullable K key, @Nullable V value);

  /**
   * Removes a key-value pair from the multimap.
   *
   * @param key key of entry to remove from the multimap
   * @param value value of entry to remove the multimap
   * @return {@code true} if the multimap changed
   */
  boolean remove(@Nullable Object key, @Nullable Object value);

  // Bulk Operations

  /**
   * Stores a collection of values with the same key.
   *
   * @param key key to store in the multimap
   * @param values values to store in the multimap
   * @return {@code true} if the multimap changed
   */
  boolean putAll(@Nullable K key, Iterable<? extends V> values);

  /**
   * Copies all of another multimap's key-value pairs into this multimap. The
   * order in which the mappings are added is determined by
   * {@code multimap.entries()}.
   *
   * @param multimap mappings to store in this multimap
   * @return {@code true} if the multimap changed
   */
  boolean putAll(Multimap<? extends K, ? extends V> multimap);

  /**
   * Stores a collection of values with the same key, replacing any existing
   * values for that key.
   *
   * @param key key to store in the multimap
   * @param values values to store in the multimap
   * @return the collection of replaced values, or an empty collection if no
   *     values were previously associated with the key. The collection
   *     <i>may</i> be modifiable, but updating it will have no effect on the
   *     multimap.
   */
  Collection<V> replaceValues(@Nullable K key, Iterable<? extends V> values);

  /**
   * Removes all values associated with a given key.
   *
   * @param key key of entries to remove from the multimap
   * @return the collection of removed values, or an empty collection if no
   *     values were associated with the provided key. The collection
   *     <i>may</i> be modifiable, but updating it will have no effect on the
   *     multimap.
   */
  Collection<V> removeAll(@Nullable Object key);

  /**
   * Removes all key-value pairs from the multimap.
   */
  void clear();

  // Views

  /**
   * Returns a collection view of all values associated with a key. If no
   * mappings in the multimap have the provided key, an empty collection is
   * returned.
   *
   * <p>Changes to the returned collection will update the underlying multimap,
   * and vice versa.
   *
   * @param key key to search for in multimap
   * @return the collection of values that the key maps to
   */
  Collection<V> get(@Nullable K key);

  /**
   * Returns the set of all keys, each appearing once in the returned set.
   * Changes to the returned set will update the underlying multimap, and vice
   * versa.
   *
   * @return the collection of distinct keys
   */
  Set<K> keySet();

  /**
   * Returns a collection, which may contain duplicates, of all keys. The number
   * of times of key appears in the returned multiset equals the number of
   * mappings the key has in the multimap. Changes to the returned multiset will
   * update the underlying multimap, and vice versa.
   *
   * @return a multiset with keys corresponding to the distinct keys of the
   *     multimap and frequencies corresponding to the number of values that
   *     each key maps to
   */
  Multiset<K> keys();

  /**
   * Returns a collection of all values in the multimap. Changes to the returned
   * collection will update the underlying multimap, and vice versa.
   *
   * @return collection of values, which may include the same value multiple
   *     times if it occurs in multiple mappings
   */
  Collection<V> values();

  /**
   * Returns a collection of all key-value pairs. Changes to the returned
   * collection will update the underlying multimap, and vice versa. The entries
   * collection does not support the {@code add} or {@code addAll} operations.
   *
   * @return collection of map entries consisting of key-value pairs
   */
  Collection<Map.Entry<K, V>> entries();

  /**
   * Returns a map view that associates each key with the corresponding values
   * in the multimap. Changes to the returned map, such as element removal,
   * will update the underlying multimap. The map does not support
   * {@code setValue()} on its entries, {@code put}, or {@code putAll}.
   *
   * <p>The collections returned by {@code asMap().get(Object)} have the same
   * behavior as those returned by {@link #get}.
   *
   * @return a map view from a key to its collection of values
   */
  Map<K, Collection<V>> asMap();

  // Comparison and hashing

  /**
   * Compares the specified object with this multimap for equality. Two
   * multimaps are equal when their map views, as returned by {@link #asMap},
   * are also equal.
   *
   * <p>In general, two multimaps with identical key-value mappings may or may
   * not be equal, depending on the implementation. For example, two
   * {@link SetMultimap} instances with the same key-value mappings are equal,
   * but equality of two {@link ListMultimap} instances depends on the ordering
   * of the values for each key.
   *
   * <p>A non-empty {@link SetMultimap} cannot be equal to a non-empty
   * {@link ListMultimap}, since their {@link #asMap} views contain unequal
   * collections as values. However, any two empty multimaps are equal, because
   * they both have empty {@link #asMap} views.
   */
  boolean equals(@Nullable Object obj);

  /**
   * Returns the hash code for this multimap.
   *
   * <p>The hash code of a multimap is defined as the hash code of the map view,
   * as returned by {@link Multimap#asMap}.
   */
  int hashCode();
}
