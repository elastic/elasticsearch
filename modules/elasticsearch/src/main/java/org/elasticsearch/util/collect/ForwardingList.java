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
import org.elasticsearch.util.annotations.GwtIncompatible;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * A list which forwards all its method calls to another list. Subclasses should
 * override one or more methods to modify the behavior of the backing list as
 * desired per the <a
 * href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator pattern</a>.
 *
 * <p>This class does not implement {@link java.util.RandomAccess}. If the
 * delegate supports random access, the {@code ForwadingList} subclass should
 * implement the {@code RandomAccess} interface.
 *
 * @author Mike Bostock
 */
@GwtCompatible
public abstract class ForwardingList<E> extends ForwardingCollection<E>
    implements List<E> {

  @Override protected abstract List<E> delegate();

  public void add(int index, E element) {
    delegate().add(index, element);
  }

  public boolean addAll(int index, Collection<? extends E> elements) {
    return delegate().addAll(index, elements);
  }

  public E get(int index) {
    return delegate().get(index);
  }

  public int indexOf(Object element) {
    return delegate().indexOf(element);
  }

  public int lastIndexOf(Object element) {
    return delegate().lastIndexOf(element);
  }

  public ListIterator<E> listIterator() {
    return delegate().listIterator();
  }

  public ListIterator<E> listIterator(int index) {
    return delegate().listIterator(index);
  }

  public E remove(int index) {
    return delegate().remove(index);
  }

  public E set(int index, E element) {
    return delegate().set(index, element);
  }

  @GwtIncompatible("List.subList")
  public List<E> subList(int fromIndex, int toIndex) {
    return Platform.subList(delegate(), fromIndex, toIndex);
  }

  @Override public boolean equals(@Nullable Object object) {
    return object == this || delegate().equals(object);
  }

  @Override public int hashCode() {
    return delegate().hashCode();
  }
}
