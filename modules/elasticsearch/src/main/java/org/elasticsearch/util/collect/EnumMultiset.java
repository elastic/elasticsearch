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
import java.util.EnumMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * Multiset implementation backed by an {@link EnumMap}.
 *
 * @author Jared Levy
 */
@GwtCompatible
public final class EnumMultiset<E extends Enum<E>>
    extends AbstractMapBasedMultiset<E> {
  /** Creates an empty {@code EnumMultiset}. */
  public static <E extends Enum<E>> EnumMultiset<E> create(Class<E> type) {
    return new EnumMultiset<E>(type);
  }

  /**
   * Creates a new {@code EnumMultiset} containing the specified elements.
   *
   * @param elements the elements that the multiset should contain
   * @throws IllegalArgumentException if {@code elements} is empty
   */
  public static <E extends Enum<E>> EnumMultiset<E> create(
      Iterable<E> elements) {
    Iterator<E> iterator = elements.iterator();
    checkArgument(iterator.hasNext(),
        "EnumMultiset constructor passed empty Iterable");
    EnumMultiset<E> multiset
        = new EnumMultiset<E>(iterator.next().getDeclaringClass());
    Iterables.addAll(multiset, elements);
    return multiset;
  }

  private transient Class<E> type;

  /** Creates an empty {@code EnumMultiset}. */
  private EnumMultiset(Class<E> type) {
    super(new EnumMap<E, AtomicInteger>(type));
    this.type = type;
  }

  private void writeObject(ObjectOutputStream stream) throws IOException {
    stream.defaultWriteObject();
    stream.writeObject(type);
    Serialization.writeMultiset(this, stream);
  }

  /**
   * @serialData the {@code Class<E>} for the enum type, the number of distinct
   *     elements, the first element, its count, the second element, its count,
   *     and so on
   */
  private void readObject(ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    @SuppressWarnings("unchecked") // reading data stored by writeObject
    Class<E> localType = (Class<E>) stream.readObject();
    type = localType;
    setBackingMap(new EnumMap<E, AtomicInteger>(type));
    Serialization.populateMultiset(this, stream);
  }

  private static final long serialVersionUID = 0;
}
