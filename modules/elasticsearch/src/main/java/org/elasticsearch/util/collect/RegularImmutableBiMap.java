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

/**
 * Bimap with one or more mappings.
 * 
 * @author Jared Levy
 */
@GwtCompatible(serializable = true)
@SuppressWarnings("serial") // uses writeReplace(), not default serialization
class RegularImmutableBiMap<K, V> extends ImmutableBiMap<K, V> {
  final transient ImmutableMap<K, V> delegate;
  final transient ImmutableBiMap<V, K> inverse;

  RegularImmutableBiMap(ImmutableMap<K, V> delegate) {
    this.delegate = delegate;

    ImmutableMap.Builder<V, K> builder = ImmutableMap.builder();
    for (Entry<K, V> entry : delegate.entrySet()) {
      builder.put(entry.getValue(), entry.getKey());
    }
    ImmutableMap<V, K> backwardMap = builder.build();
    this.inverse = new RegularImmutableBiMap<V, K>(backwardMap, this);
  }

  RegularImmutableBiMap(ImmutableMap<K, V> delegate,
      ImmutableBiMap<V, K> inverse) {
    this.delegate = delegate;
    this.inverse = inverse;
  }

  @Override ImmutableMap<K, V> delegate() {
    return delegate;
  }

  @Override public ImmutableBiMap<V, K> inverse() {
    return inverse;
  }
}
