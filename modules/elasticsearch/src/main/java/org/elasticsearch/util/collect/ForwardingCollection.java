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

import java.util.Collection;
import java.util.Iterator;

/**
 * A collection which forwards all its method calls to another collection.
 * Subclasses should override one or more methods to modify the behavior of
 * the backing collection as desired per the <a
 * href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator pattern</a>.
 *
 * @see ForwardingObject
 * @author Kevin Bourrillion
 */
@GwtCompatible
public abstract class ForwardingCollection<E> extends ForwardingObject
    implements Collection<E> {

  @Override protected abstract Collection<E> delegate();

  public Iterator<E> iterator() {
    return delegate().iterator();
  }

  public int size() {
    return delegate().size();
  }

  public boolean removeAll(Collection<?> collection) {
    return delegate().removeAll(collection);
  }

  public boolean isEmpty() {
    return delegate().isEmpty();
  }

  public boolean contains(Object object) {
    return delegate().contains(object);
  }

  public Object[] toArray() {
    return delegate().toArray();
  }

  public <T> T[] toArray(T[] array) {
    return delegate().toArray(array);
  }

  public boolean add(E element) {
    return delegate().add(element);
  }

  public boolean remove(Object object) {
    return delegate().remove(object);
  }

  public boolean containsAll(Collection<?> collection) {
    return delegate().containsAll(collection);
  }

  public boolean addAll(Collection<? extends E> collection) {
    return delegate().addAll(collection);
  }

  public boolean retainAll(Collection<?> collection) {
    return delegate().retainAll(collection);
  }

  public void clear() {
    delegate().clear();
  }
}
