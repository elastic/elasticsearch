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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * Factory and utilities pertaining to the {@code MapConstraint} interface.
 *
 * @author Mike Bostock
 */
@GwtCompatible
class ConstrainedMap<K, V> extends ForwardingMap<K, V> {
  final Map<K, V> delegate;
  final MapConstraint<? super K, ? super V> constraint;
  private volatile Set<Entry<K, V>> entrySet;

  ConstrainedMap(
      Map<K, V> delegate, MapConstraint<? super K, ? super V> constraint) {
    this.delegate = checkNotNull(delegate);
    this.constraint = checkNotNull(constraint);
  }

  @Override protected Map<K, V> delegate() {
    return delegate;
  }
  @Override public Set<Entry<K, V>> entrySet() {
    if (entrySet == null) {
      entrySet = constrainedEntrySet(delegate.entrySet(), constraint);
    }
    return entrySet;
  }
  @Override public V put(K key, V value) {
    constraint.checkKeyValue(key, value);
    return delegate.put(key, value);
  }
  @Override public void putAll(Map<? extends K, ? extends V> map) {
    for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  private static <K, V> Entry<K, V> constrainedEntry(
      final Entry<K, V> entry,
      final MapConstraint<? super K, ? super V> constraint) {
    checkNotNull(entry);
    checkNotNull(constraint);
    return new ForwardingMapEntry<K, V>() {
      @Override protected Entry<K, V> delegate() {
        return entry;
      }
      @Override public V setValue(V value) {
        constraint.checkKeyValue(getKey(), value);
        return entry.setValue(value);
      }
    };
  }

  private static <K, V> Set<Entry<K, V>> constrainedEntrySet(
      Set<Entry<K, V>> entries,
      MapConstraint<? super K, ? super V> constraint) {
    return new ConstrainedEntrySet<K, V>(entries, constraint);
  }

  private static class ConstrainedEntries<K, V>
      extends ForwardingCollection<Entry<K, V>> {
    final MapConstraint<? super K, ? super V> constraint;
    final Collection<Entry<K, V>> entries;

    ConstrainedEntries(Collection<Entry<K, V>> entries,
        MapConstraint<? super K, ? super V> constraint) {
      this.entries = entries;
      this.constraint = constraint;
    }
    @Override protected Collection<Entry<K, V>> delegate() {
      return entries;
    }

    @Override public Iterator<Entry<K, V>> iterator() {
      final Iterator<Entry<K, V>> iterator = entries.iterator();
      return new ForwardingIterator<Entry<K, V>>() {
        @Override public Entry<K, V> next() {
          return constrainedEntry(iterator.next(), constraint);
        }
        @Override protected Iterator<Entry<K, V>> delegate() {
          return iterator;
        }
      };
    }

    // See Collections.CheckedMap.CheckedEntrySet for details on attacks.

    @Override public Object[] toArray() {
      return ObjectArrays.toArrayImpl(this);
    }
    @Override public <T> T[] toArray(T[] array) {
      return ObjectArrays.toArrayImpl(this, array);
    }
    @Override public boolean contains(Object o) {
      return Maps.containsEntryImpl(delegate(), o);
    }
    @Override public boolean containsAll(Collection<?> c) {
      return Collections2.containsAll(this, c);
    }
    @Override public boolean remove(Object o) {
      return Maps.removeEntryImpl(delegate(), o);
    }
    @Override public boolean removeAll(Collection<?> c) {
      return Iterators.removeAll(iterator(), c);
    }
    @Override public boolean retainAll(Collection<?> c) {
      return Iterators.retainAll(iterator(), c);
    }
  }

  static class ConstrainedEntrySet<K, V>
      extends ConstrainedEntries<K, V> implements Set<Entry<K, V>> {
    ConstrainedEntrySet(Set<Entry<K, V>> entries,
        MapConstraint<? super K, ? super V> constraint) {
      super(entries, constraint);
    }

    // See Collections.CheckedMap.CheckedEntrySet for details on attacks.

    @Override public boolean equals(@Nullable Object object) {
      return Collections2.setEquals(this, object);
    }

    @Override public int hashCode() {
      return Sets.hashCodeImpl(this);
    }
  }
}
