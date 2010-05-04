/*
 * Copyright (C) 2008 Google Inc.
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
import org.elasticsearch.util.collect.Serialization.FieldSetter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * An immutable hash-based multiset. Does not permit null elements.
 *
 * <p>Its iterator orders elements according to the first appearance of the
 * element among the items passed to the factory method or builder. When the
 * multiset contains multiple instances of an element, those instances are
 * consecutive in the iteration order.
 *
 * @author Jared Levy
 */
@GwtCompatible(serializable = true)
public class ImmutableMultiset<E> extends ImmutableCollection<E>
    implements Multiset<E> {

  /**
   * Returns the empty immutable multiset.
   */
  @SuppressWarnings("unchecked") // all supported methods are covariant
  public static <E> ImmutableMultiset<E> of() {
    return (ImmutableMultiset<E>) EmptyImmutableMultiset.INSTANCE;
  }

  /**
   * Returns an immutable multiset containing the given elements.
   *
   * <p>The multiset is ordered by the first occurrence of each element. For
   * example, {@code ImmutableMultiset.of(2, 3, 1, 3)} yields a multiset with
   * elements in the order {@code 2, 3, 3, 1}.
   *
   * @throws NullPointerException if any of {@code elements} is null
   */
  public static <E> ImmutableMultiset<E> of(E... elements) {
    return copyOf(Arrays.asList(elements));
  }

  /**
   * Returns an immutable multiset containing the given elements.
   *
   * <p>The multiset is ordered by the first occurrence of each element. For
   * example, {@code ImmutableMultiset.copyOf(Arrays.asList(2, 3, 1, 3))} yields
   * a multiset with elements in the order {@code 2, 3, 3, 1}.
   *
   * <p>Note that if {@code c} is a {@code Collection<String>}, then {@code
   * ImmutableMultiset.copyOf(c)} returns an {@code ImmutableMultiset<String>}
   * containing each of the strings in {@code c}, while
   * {@code ImmutableMultiset.of(c)} returns an
   * {@code ImmutableMultiset<Collection<String>>} containing one element (the
   * given collection itself).
   *
   * <p><b>Note:</b> Despite what the method name suggests, if {@code elements}
   * is an {@code ImmutableMultiset}, no copy will actually be performed, and
   * the given multiset itself will be returned.
   *
   * @throws NullPointerException if any of {@code elements} is null
   */
  public static <E> ImmutableMultiset<E> copyOf(
      Iterable<? extends E> elements) {
    if (elements instanceof ImmutableMultiset) {
      @SuppressWarnings("unchecked") // all supported methods are covariant
      ImmutableMultiset<E> result = (ImmutableMultiset<E>) elements;
      return result;
    }

    @SuppressWarnings("unchecked") // the cast causes a warning
    Multiset<? extends E> multiset = (elements instanceof Multiset)
        ? (Multiset<? extends E>) elements
        : LinkedHashMultiset.create(elements);

    return copyOfInternal(multiset);
  }

  private static <E> ImmutableMultiset<E> copyOfInternal(
      Multiset<? extends E> multiset) {
    long size = 0;
    ImmutableMap.Builder<E, Integer> builder = ImmutableMap.builder();

    for (Entry<? extends E> entry : multiset.entrySet()) {
      int count = entry.getCount();
      if (count > 0) {
        // Since ImmutableMap.Builder throws an NPE if an element is null, no
        // other null checks are needed.
        builder.put(entry.getElement(), count);
        size += count;
      }
    }

    if (size == 0) {
      return of();
    }
    return new ImmutableMultiset<E>(
        builder.build(), (int) Math.min(size, Integer.MAX_VALUE));
  }

  /**
   * Returns an immutable multiset containing the given elements.
   *
   * <p>The multiset is ordered by the first occurrence of each element. For
   * example,
   * {@code ImmutableMultiset.copyOf(Arrays.asList(2, 3, 1, 3).iterator())}
   * yields a multiset with elements in the order {@code 2, 3, 3, 1}.
   *
   * @throws NullPointerException if any of {@code elements} is null
   */
  public static <E> ImmutableMultiset<E> copyOf(
      Iterator<? extends E> elements) {
    Multiset<E> multiset = LinkedHashMultiset.create();
    Iterators.addAll(multiset, elements);
    return copyOfInternal(multiset);
  }

  private final transient ImmutableMap<E, Integer> map;
  private final transient int size;

  // These constants allow the deserialization code to set final fields. This
  // holder class makes sure they are not initialized unless an instance is
  // deserialized.
  @SuppressWarnings("unchecked")
  // eclipse doesn't like the raw types here, but they're harmless
  private static class FieldSettersHolder {
    static final FieldSetter<ImmutableMultiset> MAP_FIELD_SETTER
        = Serialization.getFieldSetter(ImmutableMultiset.class, "map");
    static final FieldSetter<ImmutableMultiset> SIZE_FIELD_SETTER
        = Serialization.getFieldSetter(ImmutableMultiset.class, "size");
  }

  ImmutableMultiset(ImmutableMap<E, Integer> map, int size) {
    this.map = map;
    this.size = size;
  }

  public int count(@Nullable Object element) {
    Integer value = map.get(element);
    return (value == null) ? 0 : value;
  }

  @Override public UnmodifiableIterator<E> iterator() {
    final Iterator<Map.Entry<E, Integer>> mapIterator
        = map.entrySet().iterator();

    return new UnmodifiableIterator<E>() {
      int remaining;
      E element;

      public boolean hasNext() {
        return (remaining > 0) || mapIterator.hasNext();
      }

      public E next() {
        if (remaining <= 0) {
          Map.Entry<E, Integer> entry = mapIterator.next();
          element = entry.getKey();
          remaining = entry.getValue();
        }
        remaining--;
        return element;
      }
    };
  }

  public int size() {
    return size;
  }

  @Override public boolean contains(@Nullable Object element) {
    return map.containsKey(element);
  }

  /**
   * Guaranteed to throw an exception and leave the collection unmodified.
   *
   * @throws UnsupportedOperationException always
   */
  public int add(E element, int occurrences) {
    throw new UnsupportedOperationException();
  }

  /**
   * Guaranteed to throw an exception and leave the collection unmodified.
   *
   * @throws UnsupportedOperationException always
   */
  public int remove(Object element, int occurrences) {
    throw new UnsupportedOperationException();
  }

  /**
   * Guaranteed to throw an exception and leave the collection unmodified.
   *
   * @throws UnsupportedOperationException always
   */
  public int setCount(E element, int count) {
    throw new UnsupportedOperationException();
  }

  /**
   * Guaranteed to throw an exception and leave the collection unmodified.
   *
   * @throws UnsupportedOperationException always
   */
  public boolean setCount(E element, int oldCount, int newCount) {
    throw new UnsupportedOperationException();
  }

  @Override public boolean equals(@Nullable Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof Multiset) {
      Multiset<?> that = (Multiset<?>) object;
      if (this.size() != that.size()) {
        return false;
      }
      for (Entry<?> entry : that.entrySet()) {
        if (count(entry.getElement()) != entry.getCount()) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override public int hashCode() {
    // could cache this, but not considered worthwhile to do so
    return map.hashCode();
  }

  @Override public String toString() {
    return entrySet().toString();
  }

  // TODO: Serialization of the element set should serialize the multiset, and
  // deserialization should call multiset.elementSet(). Then
  // reserialized(multiset).elementSet() == reserialized(multiset.elementSet())
  // Currently, those object references differ.
  public Set<E> elementSet() {
    return map.keySet();
  }

  private transient ImmutableSet<Entry<E>> entrySet;

  public Set<Entry<E>> entrySet() {
    ImmutableSet<Entry<E>> es = entrySet;
    return (es == null) ? (entrySet = new EntrySet<E>(this)) : es;
  }

  private static class EntrySet<E> extends ImmutableSet<Entry<E>> {
    final ImmutableMultiset<E> multiset;

    public EntrySet(ImmutableMultiset<E> multiset) {
      this.multiset = multiset;
    }

    @Override public UnmodifiableIterator<Entry<E>> iterator() {
      final Iterator<Map.Entry<E, Integer>> mapIterator
          = multiset.map.entrySet().iterator();
      return new UnmodifiableIterator<Entry<E>>() {
        public boolean hasNext() {
          return mapIterator.hasNext();
        }
        public Entry<E> next() {
          Map.Entry<E, Integer> mapEntry = mapIterator.next();
          return
              Multisets.immutableEntry(mapEntry.getKey(), mapEntry.getValue());
        }
      };
    }

    public int size() {
      return multiset.map.size();
    }

    @Override public boolean contains(Object o) {
      if (o instanceof Entry) {
        Entry<?> entry = (Entry<?>) o;
        if (entry.getCount() <= 0) {
          return false;
        }
        int count = multiset.count(entry.getElement());
        return count == entry.getCount();
      }
      return false;
    }

    @Override public int hashCode() {
      return multiset.map.hashCode();
    }

    @Override Object writeReplace() {
      return this;
    }

    private static final long serialVersionUID = 0;
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
    int entryCount = stream.readInt();
    ImmutableMap.Builder<E, Integer> builder = ImmutableMap.builder();
    long tmpSize = 0;
    for (int i = 0; i < entryCount; i++) {
      @SuppressWarnings("unchecked") // reading data stored by writeMultiset
      E element = (E) stream.readObject();
      int count = stream.readInt();
      if (count <= 0) {
        throw new InvalidObjectException("Invalid count " + count);
      }
      builder.put(element, count);
      tmpSize += count;
    }

    FieldSettersHolder.MAP_FIELD_SETTER.set(this, builder.build());
    FieldSettersHolder.SIZE_FIELD_SETTER.set(
        this, (int) Math.min(tmpSize, Integer.MAX_VALUE));
  }

  @Override Object writeReplace() {
    return this;
  }

  private static final long serialVersionUID = 0;

  /**
   * Returns a new builder. The generated builder is equivalent to the builder
   * created by the {@link Builder} constructor.
   */
  public static <E> Builder<E> builder() {
    return new Builder<E>();
  }

  /**
   * A builder for creating immutable multiset instances, especially
   * {@code public static final} multisets ("constant multisets").
   *
   * <p>Example:
   * <pre>   {@code
   *   public static final ImmutableMultiset<Bean> BEANS
   *       = new ImmutableMultiset.Builder<Bean>()
   *           .addCopies(Bean.COCOA, 4)
   *           .addCopies(Bean.GARDEN, 6)
   *           .addCopies(Bean.RED, 8)
   *           .addCopies(Bean.BLACK_EYED, 10)
   *           .build();}</pre>
   *
   * <p>Builder instances can be reused - it is safe to call {@link #build}
   * multiple times to build multiple multisets in series. Each multiset
   * is a superset of the multiset created before it.
   */
  public static final class Builder<E> extends ImmutableCollection.Builder<E> {
    private final Multiset<E> contents = LinkedHashMultiset.create();

    /**
     * Creates a new builder. The returned builder is equivalent to the builder
     * generated by {@link ImmutableMultiset#builder}.
     */
    public Builder() {}

    /**
     * Adds {@code element} to the {@code ImmutableMultiset}.
     *
     * @param element the element to add
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code element} is null
     */
    @Override public Builder<E> add(E element) {
      contents.add(checkNotNull(element));
      return this;
    }

    /**
     * Adds a number of occurrences of an element to this {@code
     * ImmutableMultiset}.
     *
     * @param element the element to add
     * @param occurrences the number of occurrences of the element to add. May
     *     be zero, in which case no change will be made.
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code element} is null
     * @throws IllegalArgumentException if {@code occurrences} is negative, or
     *     if this operation would result in more than {@link Integer#MAX_VALUE}
     *     occurrences of the element
     */
    public Builder<E> addCopies(E element, int occurrences) {
      contents.add(checkNotNull(element), occurrences);
      return this;
    }

    /**
     * Adds or removes the necessary occurrences of an element such that the
     * element attains the desired count.
     *
     * @param element the element to add or remove occurrences of
     * @param count the desired count of the element in this multiset
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code element} is null
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public Builder<E> setCount(E element, int count) {
      contents.setCount(checkNotNull(element), count);
      return this;
    }

    /**
     * Adds each element of {@code elements} to the {@code ImmutableMultiset}.
     *
     * @param elements the elements to add
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code elements} is null or contains a
     *     null element
     */
    @Override public Builder<E> add(E... elements) {
      super.add(elements);
      return this;
    }

    /**
     * Adds each element of {@code elements} to the {@code ImmutableMultiset}.
     *
     * @param elements the {@code Iterable} to add to the {@code
     *     ImmutableMultiset}
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code elements} is null or contains a
     *     null element
     */
    @Override public Builder<E> addAll(Iterable<? extends E> elements) {
      if (elements instanceof Multiset) {
        @SuppressWarnings("unchecked")
        Multiset<? extends E> multiset = (Multiset<? extends E>) elements;
        for (Entry<? extends E> entry : multiset.entrySet()) {
          addCopies(entry.getElement(), entry.getCount());
        }
      } else {
        super.addAll(elements);
      }
      return this;
    }

    /**
     * Adds each element of {@code elements} to the {@code ImmutableMultiset}.
     *
     * @param elements the elements to add to the {@code ImmutableMultiset}
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code elements} is null or contains a
     *     null element
     */
    @Override public Builder<E> addAll(Iterator<? extends E> elements) {
      super.addAll(elements);
      return this;
    }

    /**
     * Returns a newly-created {@code ImmutableMultiset} based on the contents
     * of the {@code Builder}.
     */
    @Override public ImmutableMultiset<E> build() {
      return copyOf(contents);
    }
  }
}
