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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link BiMap} backed by two {@link HashMap} instances. This implementation
 * allows null keys and values. A {@code HashBiMap} and its inverse are both
 * serializable.
 *
 * @author Mike Bostock
 */
@GwtCompatible
public final class HashBiMap<K, V> extends AbstractBiMap<K, V> {

  /**
   * Returns a new, empty {@code HashBiMap} with the default initial capacity
   * (16).
   */
  public static <K, V> HashBiMap<K, V> create() {
    return new HashBiMap<K, V>();
  }

  /**
   * Constructs a new, empty bimap with the specified expected size.
   *
   * @param expectedSize the expected number of entries
   * @throws IllegalArgumentException if the specified expected size is
   *     negative
   */
  public static <K, V> HashBiMap<K, V> create(int expectedSize) {
    return new HashBiMap<K, V>(expectedSize);
  }

  /**
   * Constructs a new bimap containing initial values from {@code map}. The
   * bimap is created with an initial capacity sufficient to hold the mappings
   * in the specified map.
   */
  public static <K, V> HashBiMap<K, V> create(
      Map<? extends K, ? extends V> map) {
    HashBiMap<K, V> bimap = create(map.size());
    bimap.putAll(map);
    return bimap;
  }

  private HashBiMap() {
    super(new HashMap<K, V>(), new HashMap<V, K>());
  }

  private HashBiMap(int expectedSize) {
    super(new HashMap<K, V>(Maps.capacity(expectedSize)),
        new HashMap<V, K>(Maps.capacity(expectedSize)));
  }

  // Override these two methods to show that keys and values may be null

  @Override public V put(@Nullable K key, @Nullable V value) {
    return super.put(key, value);
  }

  @Override public V forcePut(@Nullable K key, @Nullable V value) {
    return super.forcePut(key, value);
  }

  /**
   * @serialData the number of entries, first key, first value, second key,
   *     second value, and so on.
   */
  private void writeObject(ObjectOutputStream stream) throws IOException {
    stream.defaultWriteObject();
    Serialization.writeMap(this, stream);
  }

  private void readObject(ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    int size = Serialization.readCount(stream);
    setDelegates(Maps.<K, V>newHashMapWithExpectedSize(size),
        Maps.<V, K>newHashMapWithExpectedSize(size));
    Serialization.populateMap(this, stream, size);
  }

  private static final long serialVersionUID = 0;
}
