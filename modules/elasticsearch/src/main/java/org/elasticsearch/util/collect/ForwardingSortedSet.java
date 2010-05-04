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

import java.util.Comparator;
import java.util.SortedSet;

/**
 * A sorted set which forwards all its method calls to another sorted set.
 * Subclasses should override one or more methods to modify the behavior of the
 * backing sorted set as desired per the <a
 * href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator pattern</a>.
 *
 * @see ForwardingObject
 * @author Mike Bostock
 */
@GwtCompatible
public abstract class ForwardingSortedSet<E> extends ForwardingSet<E>
    implements SortedSet<E> {

  @Override protected abstract SortedSet<E> delegate();

  public Comparator<? super E> comparator() {
    return delegate().comparator();
  }

  public E first() {
    return delegate().first();
  }

  public SortedSet<E> headSet(E toElement) {
    return delegate().headSet(toElement);
  }

  public E last() {
    return delegate().last();
  }

  public SortedSet<E> subSet(E fromElement, E toElement) {
    return delegate().subSet(fromElement, toElement);
  }

  public SortedSet<E> tailSet(E fromElement) {
    return delegate().tailSet(fromElement);
  }
}
