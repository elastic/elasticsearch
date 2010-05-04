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
import java.util.List;
import java.util.Map;

/**
 * Basic implementation of the {@link ListMultimap} interface. It's a wrapper
 * around {@link AbstractMultimap} that converts the returned collections into
 * {@code Lists}. The {@link #createCollection} method must return a {@code
 * List}.
 *
 * @author Jared Levy
 */
@GwtCompatible
abstract class AbstractListMultimap<K, V>
    extends AbstractMultimap<K, V> implements ListMultimap<K, V> {
  /**
   * Creates a new multimap that uses the provided map.
   *
   * @param map place to store the mapping from each key to its corresponding
   *     values
   */
  protected AbstractListMultimap(Map<K, Collection<V>> map) {
    super(map);
  }

  @Override abstract List<V> createCollection();

  @Override public List<V> get(@Nullable K key) {
    return (List<V>) super.get(key);
  }

  @Override public List<V> removeAll(@Nullable Object key) {
    return (List<V>) super.removeAll(key);
  }

  @Override public List<V> replaceValues(
      @Nullable K key, Iterable<? extends V> values) {
    return (List<V>) super.replaceValues(key, values);
  }

  /**
   * Stores a key-value pair in the multimap.
   *
   * @param key key to store in the multimap
   * @param value value to store in the multimap
   * @return {@code true} always
   */
  @Override public boolean put(@Nullable K key, @Nullable V value) {
    return super.put(key, value);
  }

  /**
   * Compares the specified object to this multimap for equality.
   *
   * <p>Two {@code ListMultimap} instances are equal if, for each key, they
   * contain the same values in the same order. If the value orderings disagree,
   * the multimaps will not be considered equal.
   */
  @Override public boolean equals(@Nullable Object object) {
    return super.equals(object);
  }
  
  private static final long serialVersionUID = 6588350623831699109L;  
}
