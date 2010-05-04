/*
 * Copyright (C) 2009 Google Inc.
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

import java.io.Serializable;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

/**
 * Implementation of {@link ImmutableSet} backed by a non-empty {@link
 * java.util.EnumSet}.
 *
 * @author Jared Levy
 */
@GwtCompatible(serializable = true)
@SuppressWarnings("serial") // we're overriding default serialization
final class ImmutableEnumSet<E /*extends Enum<E>*/> extends ImmutableSet<E> {
  /*
   * Notes on EnumSet and <E extends Enum<E>>:
   *
   * This class isn't an arbitrary ForwardingImmutableSet because we need to
   * know that calling {@code clone()} during deserialization will return an
   * object that no one else has a reference to, allowing us to guarantee
   * immutability. Hence, we support only {@link EnumSet}.
   *
   * GWT complicates matters. If we declare the class's type parameter as
   * <E extends Enum<E>> (as is necessary to declare a field of type
   * EnumSet<E>), GWT generates serializers for every available enum. This
   * increases the size of some applications' JavaScript by over 10%. To avoid
   * this, we declare the type parameter as just <E> and the field as just
   * Set<E>. writeReplace() must then use an unchecked cast to return to
   * EnumSet, guaranteeing immutability as described above.
   */
  private final transient Set<E> delegate;

  ImmutableEnumSet(Set<E> delegate) {
    this.delegate = delegate;
  }

  @Override public UnmodifiableIterator<E> iterator() {
    return Iterators.unmodifiableIterator(delegate.iterator());
  }

  public int size() {
    return delegate.size();
  }

  @Override public boolean contains(Object object) {
    return delegate.contains(object);
  }

  @Override public boolean containsAll(Collection<?> collection) {
    return delegate.containsAll(collection);
  }

  @Override public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override public Object[] toArray() {
    return delegate.toArray();
  }

  @Override public <T> T[] toArray(T[] array) {
    return delegate.toArray(array);
  }

  @Override public boolean equals(Object object) {
    return object == this || delegate.equals(object);
  }

  private transient int hashCode;

  @Override public int hashCode() {
    int result = hashCode;
    return (result == 0) ? hashCode = delegate.hashCode() : result;
  }

  @Override public String toString() {
    return delegate.toString();
  }

  // All callers of the constructor are restricted to <E extends Enum<E>>.
  @SuppressWarnings("unchecked")
  @Override Object writeReplace() {
    return new EnumSerializedForm((EnumSet) delegate);
  }

  /*
   * This class is used to serialize ImmutableEnumSet instances.
   */
  private static class EnumSerializedForm<E extends Enum<E>>
      implements Serializable {
    final EnumSet<E> delegate;
    EnumSerializedForm(EnumSet<E> delegate) {
      this.delegate = delegate;
    }
    Object readResolve() {
      // EJ2 #76: Write readObject() methods defensively.
      return new ImmutableEnumSet<E>(delegate.clone());
    }
    private static final long serialVersionUID = 0;
  }
}
