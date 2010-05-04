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

package org.elasticsearch.util.collect;

import org.elasticsearch.util.annotations.GwtCompatible;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * An empty immutable map.
 * 
 * @author Jesse Wilson
 * @author Kevin Bourrillion
 */
@GwtCompatible(serializable = true)
final class EmptyImmutableMap extends ImmutableMap<Object, Object> {
  static final EmptyImmutableMap INSTANCE = new EmptyImmutableMap();

  private EmptyImmutableMap() {}

  @Override public Object get(Object key) {
    return null;
  }

  public int size() {
    return 0;
  }

  @Override public boolean isEmpty() {
    return true;
  }

  @Override public boolean containsKey(Object key) {
    return false;
  }

  @Override public boolean containsValue(Object value) {
    return false;
  }

  @Override public ImmutableSet<Entry<Object, Object>> entrySet() {
    return ImmutableSet.of();
  }

  @Override public ImmutableSet<Object> keySet() {
    return ImmutableSet.of();
  }

  @Override public ImmutableCollection<Object> values() {
    return ImmutableCollection.EMPTY_IMMUTABLE_COLLECTION;
  }

  @Override public boolean equals(@Nullable Object object) {
    if (object instanceof Map) {
      Map<?, ?> that = (Map<?, ?>) object;
      return that.isEmpty();
    }
    return false;
  }

  @Override public int hashCode() {
    return 0;
  }

  @Override public String toString() {
    return "{}";
  }

  Object readResolve() {
    return INSTANCE; // preserve singleton property
  }

  private static final long serialVersionUID = 0;
}
