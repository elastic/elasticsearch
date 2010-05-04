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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@code Multiset} implementation with predictable iteration order. Its
 * iterator orders elements according to when the first occurrence of the
 * element was added. When the multiset contains multiple instances of an
 * element, those instances are consecutive in the iteration order. If all
 * occurrences of an element are removed, after which that element is added to
 * the multiset, the element will appear at the end of the iteration.
 *
 * @author Kevin Bourrillion
 * @author Jared Levy
 */
@GwtCompatible(serializable = true)
@SuppressWarnings("serial") // we're overriding default serialization
public final class LinkedHashMultiset<E> extends AbstractMapBasedMultiset<E> {

  /**
   * Creates a new, empty {@code LinkedHashMultiset} using the default initial
   * capacity.
   */
  public static <E> LinkedHashMultiset<E> create() {
    return new LinkedHashMultiset<E>();
  }

  /**
   * Creates a new, empty {@code LinkedHashMultiset} with the specified expected
   * number of distinct elements.
   *
   * @param distinctElements the expected number of distinct elements
   * @throws IllegalArgumentException if {@code distinctElements} is negative
   */
  public static <E> LinkedHashMultiset<E> create(int distinctElements) {
    return new LinkedHashMultiset<E>(distinctElements);
  }

  /**
   * Creates a new {@code LinkedHashMultiset} containing the specified elements.
   *
   * @param elements the elements that the multiset should contain
   */
  public static <E> LinkedHashMultiset<E> create(
      Iterable<? extends E> elements) {
    LinkedHashMultiset<E> multiset =
        create(Multisets.inferDistinctElements(elements));
    Iterables.addAll(multiset, elements);
    return multiset;
  }

  private LinkedHashMultiset() {
    super(new LinkedHashMap<E, AtomicInteger>());
  }

  private LinkedHashMultiset(int distinctElements) {
    // Could use newLinkedHashMapWithExpectedSize() if it existed
    super(new LinkedHashMap<E, AtomicInteger>(Maps.capacity(distinctElements)));
  }

  /**
   * @serialData the number of distinct elements, the first element, its count,
   *     the second element, its count, and so on
   */
  private void writeObject(ObjectOutputStream stream) throws IOException {
    stream.defaultWriteObject();
    Serialization.writeMultiset(this, stream);
  }

  private void readObject(ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    int distinctElements = Serialization.readCount(stream);
    setBackingMap(new LinkedHashMap<E, AtomicInteger>(
        Maps.capacity(distinctElements)));
    Serialization.populateMultiset(this, stream, distinctElements);
  }

  private static final long serialVersionUID = 0;
}
