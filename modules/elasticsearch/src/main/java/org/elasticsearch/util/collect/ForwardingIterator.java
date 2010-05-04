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

import java.util.Iterator;

/**
 * An iterator which forwards all its method calls to another iterator.
 * Subclasses should override one or more methods to modify the behavior of the
 * backing iterator as desired per the <a
 * href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator pattern</a>.
 *
 * @see ForwardingObject
 * @author Kevin Bourrillion
 */
@GwtCompatible
public abstract class ForwardingIterator<T>
    extends ForwardingObject implements Iterator<T> {

  @Override protected abstract Iterator<T> delegate();

  public boolean hasNext() {
    return delegate().hasNext();
  }

  public T next() {
    return delegate().next();
  }

  public void remove() {
    delegate().remove();
  }
}
