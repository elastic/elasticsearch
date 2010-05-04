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

import java.util.concurrent.ConcurrentMap;

/**
 * A concurrent map which forwards all its method calls to another concurrent
 * map. Subclasses should override one or more methods to modify the behavior of
 * the backing map as desired per the <a
 * href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator pattern</a>.
 *
 * @see ForwardingObject
 * @author Charles Fry
 */
@GwtCompatible
public abstract class ForwardingConcurrentMap<K, V> extends ForwardingMap<K, V>
    implements ConcurrentMap<K, V> {

  @Override protected abstract ConcurrentMap<K, V> delegate();

  public V putIfAbsent(K key, V value) {
    return delegate().putIfAbsent(key, value);
  }

  public boolean remove(Object key, Object value) {
    return delegate().remove(key, value);
  }

  public V replace(K key, V value) {
    return delegate().replace(key, value);
  }

  public boolean replace(K key, V oldValue, V newValue) {
    return delegate().replace(key, oldValue, newValue);
  }

}
