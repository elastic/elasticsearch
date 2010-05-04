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

import java.util.NoSuchElementException;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * This class provides a skeletal implementation of the {@code Iterator}
 * interface, to make this interface easier to implement for certain types of
 * data sources.
 *
 * <p>{@code Iterator} requires its implementations to support querying the
 * end-of-data status without changing the iterator's state, using the {@link
 * #hasNext} method. But many data sources, such as {@link
 * java.io.Reader#read()}), do not expose this information; the only way to
 * discover whether there is any data left is by trying to retrieve it. These
 * types of data sources are ordinarily difficult to write iterators for. But
 * using this class, one must implement only the {@link #computeNext} method,
 * and invoke the {@link #endOfData} method when appropriate.
 *
 * <p>Another example is an iterator that skips over null elements in a backing
 * iterator. This could be implemented as: <pre>   {@code
 *
 *   public static Iterator<String> skipNulls(final Iterator<String> in) {
 *     return new AbstractIterator<String>() {
 *       protected String computeNext() {
 *         while (in.hasNext()) {
 *           String s = in.next();
 *           if (s != null) {
 *             return s;
 *           }
 *         }
 *         return endOfData();
 *       }
 *     };
 *   }}</pre>
 *
 * This class supports iterators that include null elements.
 *
 * @author Kevin Bourrillion
 */
@GwtCompatible
public abstract class AbstractIterator<T> extends UnmodifiableIterator<T> {
  private State state = State.NOT_READY;

  private enum State {
    /** We have computed the next element and haven't returned it yet. */
    READY,

    /** We haven't yet computed or have already returned the element. */
    NOT_READY,

    /** We have reached the end of the data and are finished. */
    DONE,

    /** We've suffered an exception and are kaput. */
    FAILED,
  }

  private T next;

  /**
   * Returns the next element. <b>Note:</b> the implementation must call {@link
   * #endOfData()} when there are no elements left in the iteration. Failure to
   * do so could result in an infinite loop.
   *
   * <p>The initial invocation of {@link #hasNext()} or {@link #next()} calls
   * this method, as does the first invocation of {@code hasNext} or {@code
   * next} following each successful call to {@code next}. Once the
   * implementation either invokes {@code endOfData} or throws an exception,
   * {@code computeNext} is guaranteed to never be called again.
   *
   * <p>If this method throws an exception, it will propagate outward to the
   * {@code hasNext} or {@code next} invocation that invoked this method. Any
   * further attempts to use the iterator will result in an {@link
   * IllegalStateException}.
   *
   * <p>The implementation of this method may not invoke the {@code hasNext},
   * {@code next}, or {@link #peek()} methods on this instance; if it does, an
   * {@code IllegalStateException} will result.
   *
   * @return the next element if there was one. If {@code endOfData} was called
   *     during execution, the return value will be ignored.
   * @throws RuntimeException if any unrecoverable error happens. This exception
   *     will propagate outward to the {@code hasNext()}, {@code next()}, or
   *     {@code peek()} invocation that invoked this method. Any further
   *     attempts to use the iterator will result in an
   *     {@link IllegalStateException}.
   */
  protected abstract T computeNext();

  /**
   * Implementations of {@code computeNext} <b>must</b> invoke this method when
   * there are no elements left in the iteration.
   *
   * @return {@code null}; a convenience so your {@link #computeNext}
   *     implementation can use the simple statement {@code return endOfData();}
   */
  protected final T endOfData() {
    state = State.DONE;
    return null;
  }

  public final boolean hasNext() {
    checkState(state != State.FAILED);
    switch (state) {
      case DONE:
        return false;
      case READY:
        return true;
      default:
    }
    return tryToComputeNext();
  }

  private boolean tryToComputeNext() {
    state = State.FAILED; // temporary pessimism
    next = computeNext();
    if (state != State.DONE) {
      state = State.READY;
      return true;
    }
    return false;
  }

  public final T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    state = State.NOT_READY;
    return next;
  }

  /**
   * Returns the next element in the iteration without advancing the iteration,
   * according to the contract of {@link PeekingIterator#peek()}.
   *
   * <p>Implementations of {@code AbstractIterator} that wish to expose this
   * functionality should implement {@code PeekingIterator}.
   */
  public final T peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return next;
  }
}
