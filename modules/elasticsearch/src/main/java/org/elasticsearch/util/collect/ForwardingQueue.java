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

import java.util.Queue;

/**
 * A queue which forwards all its method calls to another queue. Subclasses
 * should override one or more methods to modify the behavior of the backing
 * queue as desired per the <a
 * href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator pattern</a>.
 *
 * @see ForwardingObject
 * @author Mike Bostock
 */
@GwtCompatible
public abstract class ForwardingQueue<E> extends ForwardingCollection<E>
    implements Queue<E> {

  @Override protected abstract Queue<E> delegate();

  public boolean offer(E o) {
    return delegate().offer(o);
  }

  public E poll() {
    return delegate().poll();
  }

  public E remove() {
    return delegate().remove();
  }

  public E peek() {
    return delegate().peek();
  }

  public E element() {
    return delegate().element();
  }
}
