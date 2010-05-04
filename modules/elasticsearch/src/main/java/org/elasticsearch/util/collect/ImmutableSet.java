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

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * A high-performance, immutable {@code Set} with reliable, user-specified
 * iteration order. Does not permit null elements.
 *
 * <p>Unlike {@link Collections#unmodifiableSet}, which is a <i>view</i> of a
 * separate collection that can still change, an instance of this class contains
 * its own private data and will <i>never</i> change. This class is convenient
 * for {@code public static final} sets ("constant sets") and also lets you
 * easily make a "defensive copy" of a set provided to your class by a caller.
 *
 * <p><b>Warning:</b> Like most sets, an {@code ImmutableSet} will not function
 * correctly if an element is modified after being placed in the set. For this
 * reason, and to avoid general confusion, it is strongly recommended to place
 * only immutable objects into this collection.
 *
 * <p>This class has been observed to perform significantly better than {@link
 * HashSet} for objects with very fast {@link Object#hashCode} implementations
 * (as a well-behaved immutable object should). While this class's factory
 * methods create hash-based instances, the {@link ImmutableSortedSet} subclass
 * performs binary searches instead.
 *
 * <p><b>Note</b>: Although this class is not final, it cannot be subclassed
 * outside its package as it has no public or protected constructors. Thus,
 * instances of this type are guaranteed to be immutable.
 *
 * @see ImmutableList
 * @see ImmutableMap
 * @author Kevin Bourrillion
 * @author Nick Kralevich
 */
@GwtCompatible(serializable = true)
@SuppressWarnings("serial") // we're overriding default serialization
public abstract class ImmutableSet<E> extends ImmutableCollection<E>
    implements Set<E> {
  /**
   * Returns the empty immutable set. This set behaves and performs comparably
   * to {@link Collections#emptySet}, and is preferable mainly for consistency
   * and maintainability of your code.
   */
  // Casting to any type is safe because the set will never hold any elements.
  @SuppressWarnings({"unchecked"})
  public static <E> ImmutableSet<E> of() {
    return (ImmutableSet<E>) EmptyImmutableSet.INSTANCE;
  }

  /**
   * Returns an immutable set containing a single element. This set behaves and
   * performs comparably to {@link Collections#singleton}, but will not accept
   * a null element. It is preferable mainly for consistency and
   * maintainability of your code.
   */
  public static <E> ImmutableSet<E> of(E element) {
    return new SingletonImmutableSet<E>(element);
  }

  /**
   * Returns an immutable set containing the given elements, in order. Repeated
   * occurrences of an element (according to {@link Object#equals}) after the
   * first are ignored.
   *
   * @throws NullPointerException if any element is null
   */
  @SuppressWarnings("unchecked")
  public static <E> ImmutableSet<E> of(E e1, E e2) {
    return create(e1, e2);
  }

  /**
   * Returns an immutable set containing the given elements, in order. Repeated
   * occurrences of an element (according to {@link Object#equals}) after the
   * first are ignored.
   *
   * @throws NullPointerException if any element is null
   */
  @SuppressWarnings("unchecked")
  public static <E> ImmutableSet<E> of(E e1, E e2, E e3) {
    return create(e1, e2, e3);
  }

  /**
   * Returns an immutable set containing the given elements, in order. Repeated
   * occurrences of an element (according to {@link Object#equals}) after the
   * first are ignored.
   *
   * @throws NullPointerException if any element is null
   */
  @SuppressWarnings("unchecked")
  public static <E> ImmutableSet<E> of(E e1, E e2, E e3, E e4) {
    return create(e1, e2, e3, e4);
  }

  /**
   * Returns an immutable set containing the given elements, in order. Repeated
   * occurrences of an element (according to {@link Object#equals}) after the
   * first are ignored.
   *
   * @throws NullPointerException if any element is null
   */
  @SuppressWarnings("unchecked")
  public static <E> ImmutableSet<E> of(E e1, E e2, E e3, E e4, E e5) {
    return create(e1, e2, e3, e4, e5);
  }

  /**
   * Returns an immutable set containing the given elements, in order. Repeated
   * occurrences of an element (according to {@link Object#equals}) after the
   * first are ignored (but too many of these may result in the set being
   * sized inappropriately).
   *
   * @throws NullPointerException if any of {@code elements} is null
   */
  public static <E> ImmutableSet<E> of(E... elements) {
    checkNotNull(elements); // for GWT
    switch (elements.length) {
      case 0:
        return of();
      case 1:
        return of(elements[0]);
      default:
        return create(elements);
    }
  }

  /**
   * Returns an immutable set containing the given elements, in order. Repeated
   * occurrences of an element (according to {@link Object#equals}) after the
   * first are ignored (but too many of these may result in the set being
   * sized inappropriately). This method iterates over {@code elements} at most
   * once.
   *
   * <p>Note that if {@code s} is a {@code Set<String>}, then {@code
   * ImmutableSet.copyOf(s)} returns an {@code ImmutableSet<String>} containing
   * each of the strings in {@code s}, while {@code ImmutableSet.of(s)} returns
   * a {@code ImmutableSet<Set<String>>} containing one element (the given set
   * itself).
   *
   * <p><b>Note:</b> Despite what the method name suggests, if {@code elements}
   * is an {@code ImmutableSet} (but not an {@code ImmutableSortedSet}), no copy
   * will actually be performed, and the given set itself will be returned.
   *
   * @throws NullPointerException if any of {@code elements} is null
   */
  public static <E> ImmutableSet<E> copyOf(Iterable<? extends E> elements) {
    if (elements instanceof ImmutableSet
        && !(elements instanceof ImmutableSortedSet)) {
      @SuppressWarnings("unchecked") // all supported methods are covariant
      ImmutableSet<E> set = (ImmutableSet<E>) elements;
      return set;
    }
    return copyOfInternal(Collections2.toCollection(elements));
  }

  /**
   * Returns an immutable set containing the given elements, in order. Repeated
   * occurrences of an element (according to {@link Object#equals}) after the
   * first are ignored.
   *
   * @throws NullPointerException if any of {@code elements} is null
   */
  public static <E> ImmutableSet<E> copyOf(Iterator<? extends E> elements) {
    Collection<E> list = Lists.newArrayList(elements);
    return copyOfInternal(list);
  }

  private static <E> ImmutableSet<E> copyOfInternal(
      Collection<? extends E> collection) {
    // TODO: Support concurrent collections that change while this method is
    // running.
    switch (collection.size()) {
      case 0:
        return of();
      case 1:
        // TODO: Remove "ImmutableSet.<E>" when eclipse bug is fixed.
        return ImmutableSet.<E>of(collection.iterator().next());
      default:
        return create(collection, collection.size());
    }
  }

  ImmutableSet() {}

  /** Returns {@code true} if the {@code hashCode()} method runs quickly. */
  boolean isHashCodeFast() {
    return false;
  }

  @Override public boolean equals(@Nullable Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof ImmutableSet
        && isHashCodeFast()
        && ((ImmutableSet<?>) object).isHashCodeFast()
        && hashCode() != object.hashCode()) {
      return false;
    }
    return Collections2.setEquals(this, object);
  }

  @Override public int hashCode() {
    int hashCode = 0;
    for (Object o : this) {
      hashCode += o.hashCode();
    }
    return hashCode;
  }

  // This declaration is needed to make Set.iterator() and
  // ImmutableCollection.iterator() consistent.
  @Override public abstract UnmodifiableIterator<E> iterator();

  private static <E> ImmutableSet<E> create(E... elements) {
    return create(Arrays.asList(elements), elements.length);
  }

  private static <E> ImmutableSet<E> create(
      Iterable<? extends E> iterable, int count) {
    // count is always the (nonzero) number of elements in the iterable
    int tableSize = Hashing.chooseTableSize(count);
    Object[] table = new Object[tableSize];
    int mask = tableSize - 1;

    List<E> elements = new ArrayList<E>(count);
    int hashCode = 0;

    for (E element : iterable) {
      checkNotNull(element); // for GWT
      int hash = element.hashCode();
      for (int i = Hashing.smear(hash); true; i++) {
        int index = i & mask;
        Object value = table[index];
        if (value == null) {
          // Came to an empty bucket. Put the element here.
          table[index] = element;
          elements.add(element);
          hashCode += hash;
          break;
        } else if (value.equals(element)) {
          break; // Found a duplicate. Nothing to do.
        }
      }
    }

    if (elements.size() == 1) {
      // The iterable contained only duplicates of the same element.
      return new SingletonImmutableSet<E>(elements.get(0), hashCode); 
    } else if (tableSize > Hashing.chooseTableSize(elements.size())) {
      // Resize the table when the iterable includes too many duplicates.
      return create(elements, elements.size()); 
    } else {
      return new RegularImmutableSet<E>(
          elements.toArray(), hashCode, table, mask);
    }
  }

  abstract static class ArrayImmutableSet<E> extends ImmutableSet<E> {
    // the elements (two or more) in the desired order.
    final transient Object[] elements;

    ArrayImmutableSet(Object[] elements) {
      this.elements = elements;
    }

    public int size() {
      return elements.length;
    }

    @Override public boolean isEmpty() {
      return false;
    }

    /*
     * The cast is safe because the only way to create an instance is via the
     * create() method above, which only permits elements of type E.
     */
    @SuppressWarnings("unchecked")
    @Override public UnmodifiableIterator<E> iterator() {
      return (UnmodifiableIterator<E>) Iterators.forArray(elements);
    }

    @Override public Object[] toArray() {
      Object[] array = new Object[size()];
      System.arraycopy(elements, 0, array, 0, size());
      return array;
    }

    @Override public <T> T[] toArray(T[] array) {
      int size = size();
      if (array.length < size) {
        array = ObjectArrays.newArray(array, size);
      } else if (array.length > size) {
        array[size] = null;
      }
      System.arraycopy(elements, 0, array, 0, size);
      return array;
    }

    @Override public boolean containsAll(Collection<?> targets) {
      if (targets == this) {
        return true;
      }
      if (!(targets instanceof ArrayImmutableSet)) {
        return super.containsAll(targets);
      }
      if (targets.size() > size()) {
        return false;
      }
      for (Object target : ((ArrayImmutableSet<?>) targets).elements) {
        if (!contains(target)) {
          return false;
        }
      }
      return true;
    }
  }

  /** such as ImmutableMap.keySet() */
  abstract static class TransformedImmutableSet<D, E> extends ImmutableSet<E> {
    final D[] source;
    final int hashCode;

    TransformedImmutableSet(D[] source, int hashCode) {
      this.source = source;
      this.hashCode = hashCode;
    }

    abstract E transform(D element);

    public int size() {
      return source.length;
    }

    @Override public boolean isEmpty() {
      return false;
    }

    @Override public UnmodifiableIterator<E> iterator() {
      return new AbstractIterator<E>() {
        int index = 0;
        @Override protected E computeNext() {
          return index < source.length
              ? transform(source[index++])
              : endOfData();
        }
      };
    }

    @Override public Object[] toArray() {
      return toArray(new Object[size()]);
    }

    @Override public <T> T[] toArray(T[] array) {
      int size = size();
      if (array.length < size) {
        array = ObjectArrays.newArray(array, size);
      } else if (array.length > size) {
        array[size] = null;
      }

      // Writes will produce ArrayStoreException when the toArray() doc requires.
      Object[] objectArray = array;
      for (int i = 0; i < source.length; i++) {
        objectArray[i] = transform(source[i]);
      }
      return array;
    }

    @Override public final int hashCode() {
      return hashCode;
    }

    @Override boolean isHashCodeFast() {
      return true;
    }
  }

  /*
   * This class is used to serialize all ImmutableSet instances, except for
   * ImmutableEnumSet/ImmutableSortedSet, regardless of implementation type. It
   * captures their "logical contents" and they are reconstructed using public
   * static factories. This is necessary to ensure that the existence of a
   * particular implementation type is an implementation detail.
   */
  private static class SerializedForm implements Serializable {
    final Object[] elements;
    SerializedForm(Object[] elements) {
      this.elements = elements;
    }
    Object readResolve() {
      return of(elements);
    }
    private static final long serialVersionUID = 0;
  }

  @Override Object writeReplace() {
    return new SerializedForm(toArray());
  }

  /**
   * Returns a new builder. The generated builder is equivalent to the builder
   * created by the {@link Builder} constructor.
   */
  public static <E> Builder<E> builder() {
    return new Builder<E>();
  }

  /**
   * A builder for creating immutable set instances, especially
   * {@code public static final} sets ("constant sets").
   *
   * <p>Example:
   * <pre>{@code
   *   public static final ImmutableSet<Color> GOOGLE_COLORS
   *       = new ImmutableSet.Builder<Color>()
   *           .addAll(WEBSAFE_COLORS)
   *           .add(new Color(0, 191, 255))
   *           .build();}</pre>
   *
   * <p>Builder instances can be reused - it is safe to call {@link #build}
   * multiple times to build multiple sets in series. Each set
   * is a superset of the set created before it.
   */
  public static class Builder<E> extends ImmutableCollection.Builder<E> {
    // accessed directly by ImmutableSortedSet
    final ArrayList<E> contents = Lists.newArrayList();

    /**
     * Creates a new builder. The returned builder is equivalent to the builder
     * generated by {@link ImmutableSet#builder}.
     */
    public Builder() {}

    /**
     * Adds {@code element} to the {@code ImmutableSet}.  If the {@code
     * ImmutableSet} already contains {@code element}, then {@code add} has no
     * effect (only the previously added element is retained).
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
     * Adds each element of {@code elements} to the {@code ImmutableSet},
     * ignoring duplicate elements (only the first duplicate element is added).
     *
     * @param elements the elements to add
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code elements} is null or contains a
     *     null element
     */
    @Override public Builder<E> add(E... elements) {
      checkNotNull(elements); // for GWT
      contents.ensureCapacity(contents.size() + elements.length);
      super.add(elements);
      return this;
    }

    /**
     * Adds each element of {@code elements} to the {@code ImmutableSet},
     * ignoring duplicate elements (only the first duplicate element is added).
     *
     * @param elements the {@code Iterable} to add to the {@code ImmutableSet}
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code elements} is null or contains a
     *     null element
     */
    @Override public Builder<E> addAll(Iterable<? extends E> elements) {
      if (elements instanceof Collection) {
        Collection<?> collection = (Collection<?>) elements;
        contents.ensureCapacity(contents.size() + collection.size());
      }
      super.addAll(elements);
      return this;
    }

    /**
     * Adds each element of {@code elements} to the {@code ImmutableSet},
     * ignoring duplicate elements (only the first duplicate element is added).
     *
     * @param elements the elements to add to the {@code ImmutableSet}
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code elements} is null or contains a
     *     null element
     */
    @Override public Builder<E> addAll(Iterator<? extends E> elements) {
      super.addAll(elements);
      return this;
    }

    /**
     * Returns a newly-created {@code ImmutableSet} based on the contents of
     * the {@code Builder}.
     */
    @Override public ImmutableSet<E> build() {
      return copyOf(contents);
    }
  }
}
