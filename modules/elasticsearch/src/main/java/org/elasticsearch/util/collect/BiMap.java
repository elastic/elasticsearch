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
import java.util.Set;

/**
 * A bimap (or "bidirectional map") is a map that preserves the uniqueness of
 * its values as well as that of its keys. This constraint enables bimaps to
 * support an "inverse view", which is another bimap containing the same entries
 * as this bimap but with reversed keys and values.
 *
 * @author Kevin Bourrillion
 */
@GwtCompatible
public interface BiMap<K, V> extends Map<K, V> {
  // Modification Operations

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException if the given value is already bound to a
   *     different key in this bimap. The bimap will remain unmodified in this
   *     event. To avoid this exception, call {@link #forcePut} instead.
   */
  V put(@Nullable K key, @Nullable V value);

  /**
   * An alternate form of {@code put} that silently removes any existing entry
   * with the value {@code value} before proceeding with the {@link #put}
   * operation. If the bimap previously contained the provided key-value
   * mapping, this method has no effect.
   *
   * <p>Note that a successful call to this method could cause the size of the
   * bimap to increase by one, stay the same, or even decrease by one.
   *
   * <p><b>Warning</b>: If an existing entry with this value is removed, the key
   * for that entry is discarded and not returned.
   *
   * @param key the key with which the specified value is to be associated
   * @param value the value to be associated with the specified key
   * @return the value which was previously associated with the key, which may
   *     be {@code null}, or {@code null} if there was no previous entry
   */
  V forcePut(@Nullable K key, @Nullable V value);

  // Bulk Operations

  /**
   * {@inheritDoc}
   *
   * <p><b>Warning:</b> the results of calling this method may vary depending on
   * the iteration order of {@code map}.
   *
   * @throws IllegalArgumentException if an attempt to {@code put} any
   *     entry fails. Note that some map entries may have been added to the
   *     bimap before the exception was thrown.
   */
  void putAll(Map<? extends K, ? extends V> map);

  // Views

  /**
   * {@inheritDoc}
   *
   * <p>Because a bimap has unique values, this method returns a {@link Set},
   * instead of the {@link java.util.Collection} specified in the {@link Map}
   * interface.
   */
  Set<V> values();

  /**
   * Returns the inverse view of this bimap, which maps each of this bimap's
   * values to its associated key. The two bimaps are backed by the same data;
   * any changes to one will appear in the other.
   *
   * <p><b>Note:</b>There is no guaranteed correspondence between the iteration
   * order of a bimap and that of its inverse.
   *
   * @return the inverse view of this bimap
   */
  BiMap<V, K> inverse();
}
