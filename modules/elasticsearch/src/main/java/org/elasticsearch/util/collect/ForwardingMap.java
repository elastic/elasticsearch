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
 * A map which forwards all its method calls to another map. Subclasses should
 * override one or more methods to modify the behavior of the backing map as
 * desired per the <a
 * href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator pattern</a>.
 *
 * @see ForwardingObject
 * @author Kevin Bourrillion
 * @author Jared Levy
 */
@GwtCompatible
public abstract class ForwardingMap<K, V> extends ForwardingObject
    implements Map<K, V> {

  @Override protected abstract Map<K, V> delegate();

  public int size() {
    return delegate().size();
  }

  public boolean isEmpty() {
    return delegate().isEmpty();
  }

  public V remove(Object object) {
    return delegate().remove(object);
  }

  public void clear() {
    delegate().clear();
  }

  public boolean containsKey(Object key) {
    return delegate().containsKey(key);
  }

  public boolean containsValue(Object value) {
    return delegate().containsValue(value);
  }

  public V get(Object key) {
    return delegate().get(key);
  }

  public V put(K key, V value) {
    return delegate().put(key, value);
  }

  public void putAll(Map<? extends K, ? extends V> map) {
    delegate().putAll(map);
  }

  public Set<K> keySet() {
    return delegate().keySet();
  }

  public Collection<V> values() {
    return delegate().values();
  }

  public Set<Entry<K, V>> entrySet() {
    return delegate().entrySet();
  }

  @Override public boolean equals(@Nullable Object object) {
    return object == this || delegate().equals(object);
  }

  @Override public int hashCode() {
    return delegate().hashCode();
  }
}
