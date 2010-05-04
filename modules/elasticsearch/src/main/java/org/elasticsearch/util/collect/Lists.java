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
import org.elasticsearch.util.annotations.VisibleForTesting;
import org.elasticsearch.util.base.Function;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * Static utility methods pertaining to {@link List} instances. Also see this
 * class's counterparts {@link Sets} and {@link Maps}.
 *
 * @author Kevin Bourrillion
 * @author Mike Bostock
 */
@GwtCompatible
public final class Lists {
  private Lists() {}

  // ArrayList

  /**
   * Creates a <i>mutable</i>, empty {@code ArrayList} instance.
   *
   * <p><b>Note:</b> if mutability is not required, use {@link
   * ImmutableList#of()} instead.
   *
   * @return a new, empty {@code ArrayList}
   */
  @GwtCompatible(serializable = true)
  public static <E> ArrayList<E> newArrayList() {
    return new ArrayList<E>();
  }

  /**
   * Creates a <i>mutable</i> {@code ArrayList} instance containing the given
   * elements.
   *
   * <p><b>Note:</b> if mutability is not required and the elements are
   * non-null, use {@link ImmutableList#of(Object[])} instead.
   *
   * @param elements the elements that the list should contain, in order
   * @return a new {@code ArrayList} containing those elements
   */
  @GwtCompatible(serializable = true)
  public static <E> ArrayList<E> newArrayList(E... elements) {
    checkNotNull(elements); // for GWT
    // Avoid integer overflow when a large array is passed in
    int capacity = computeArrayListCapacity(elements.length);
    ArrayList<E> list = new ArrayList<E>(capacity);
    Collections.addAll(list, elements);
    return list;
  }

  @VisibleForTesting static int computeArrayListCapacity(int arraySize) {
    checkArgument(arraySize >= 0);

    // TODO: Figure out the right behavior, and document it
    return (int) Math.min(5L + arraySize + (arraySize / 10), Integer.MAX_VALUE);
  }

  /**
   * Creates a <i>mutable</i> {@code ArrayList} instance containing the given
   * elements.
   *
   * <p><b>Note:</b> if mutability is not required and the elements are
   * non-null, use {@link ImmutableList#copyOf(Iterator)} instead.
   *
   * @param elements the elements that the list should contain, in order
   * @return a new {@code ArrayList} containing those elements
   */
  @GwtCompatible(serializable = true)
  public static <E> ArrayList<E> newArrayList(Iterable<? extends E> elements) {
    checkNotNull(elements); // for GWT
    // Let ArrayList's sizing logic work, if possible
    if (elements instanceof Collection) {
      @SuppressWarnings("unchecked")
      Collection<? extends E> collection = (Collection<? extends E>) elements;
      return new ArrayList<E>(collection);
    } else {
      return newArrayList(elements.iterator());
    }
  }

  /**
   * Creates a <i>mutable</i> {@code ArrayList} instance containing the given
   * elements.
   *
   * <p><b>Note:</b> if mutability is not required and the elements are
   * non-null, use {@link ImmutableList#copyOf(Iterator)} instead.
   *
   * @param elements the elements that the list should contain, in order
   * @return a new {@code ArrayList} containing those elements
   */
  @GwtCompatible(serializable = true)
  public static <E> ArrayList<E> newArrayList(Iterator<? extends E> elements) {
    checkNotNull(elements); // for GWT
    ArrayList<E> list = newArrayList();
    while (elements.hasNext()) {
      list.add(elements.next());
    }
    return list;
  }

  /**
   * Creates an {@code ArrayList} instance backed by an array of the
   * <i>exact</i> size specified; equivalent to
   * {@link ArrayList#ArrayList(int)}.
   *
   * <p><b>Note:</b> if you know the exact size your list will be, consider
   * using a fixed-size list ({@link Arrays#asList(Object[])}) or an {@link
   * ImmutableList} instead of a growable {@link ArrayList}.
   *
   * <p><b>Note:</b> If you have only an <i>estimate</i> of the eventual size of
   * the list, consider padding this estimate by a suitable amount, or simply
   * use {@link #newArrayListWithExpectedSize(int)} instead.
   *
   * @param initialArraySize the exact size of the initial backing array for
   *     the returned array list ({@code ArrayList} documentation calls this
   *     value the "capacity")
   * @return a new, empty {@code ArrayList} which is guaranteed not to resize
   *     itself unless its size reaches {@code initialArraySize + 1}
   * @throws IllegalArgumentException if {@code initialArraySize} is negative
   */
  @GwtCompatible(serializable = true)
  public static <E> ArrayList<E> newArrayListWithCapacity(
      int initialArraySize) {
    return new ArrayList<E>(initialArraySize);
  }

  /**
   * Creates an {@code ArrayList} instance sized appropriately to hold an
   * <i>estimated</i> number of elements without resizing. A small amount of
   * padding is added in case the estimate is low.
   *
   * <p><b>Note:</b> If you know the <i>exact</i> number of elements the list
   * will hold, or prefer to calculate your own amount of padding, refer to
   * {@link #newArrayListWithCapacity(int)}.
   *
   * @param estimatedSize an estimate of the eventual {@link List#size()} of
   *     the new list
   * @return a new, empty {@code ArrayList}, sized appropriately to hold the
   *     estimated number of elements
   * @throws IllegalArgumentException if {@code estimatedSize} is negative
   */
  @GwtCompatible(serializable = true)
  public static <E> ArrayList<E> newArrayListWithExpectedSize(
      int estimatedSize) {
    return new ArrayList<E>(computeArrayListCapacity(estimatedSize));
  }

  // LinkedList

  /**
   * Creates an empty {@code LinkedList} instance.
   *
   * <p><b>Note:</b> if you need an immutable empty {@link List}, use
   * {@link Collections#emptyList} instead.
   *
   * @return a new, empty {@code LinkedList}
   */
  @GwtCompatible(serializable = true)
  public static <E> LinkedList<E> newLinkedList() {
    return new LinkedList<E>();
  }

  /**
   * Creates a {@code LinkedList} instance containing the given elements.
   *
   * @param elements the elements that the list should contain, in order
   * @return a new {@code LinkedList} containing those elements
   */
  @GwtCompatible(serializable = true)
  public static <E> LinkedList<E> newLinkedList(
      Iterable<? extends E> elements) {
    LinkedList<E> list = newLinkedList();
    for (E element : elements) {
      list.add(element);
    }
    return list;
  }

  /**
   * Returns an unmodifiable list containing the specified first element and
   * backed by the specified array of additional elements. Changes to the {@code
   * rest} array will be reflected in the returned list. Unlike {@link
   * Arrays#asList}, the returned list is unmodifiable.
   *
   * <p>This is useful when a varargs method needs to use a signature such as
   * {@code (Foo firstFoo, Foo... moreFoos)}, in order to avoid overload
   * ambiguity or to enforce a minimum argument count.
   *
   * <p>The returned list is serializable and implements {@link RandomAccess}.
   *
   * @param first the first element
   * @param rest an array of additional elements, possibly empty
   * @return an unmodifiable list containing the specified elements
   */
  public static <E> List<E> asList(@Nullable E first, E[] rest) {
    return new OnePlusArrayList<E>(first, rest);
  }

  /** @see Lists#asList(Object, Object[]) */
  private static class OnePlusArrayList<E> extends AbstractList<E>
      implements Serializable, RandomAccess {
    final E first;
    final E[] rest;

    OnePlusArrayList(@Nullable E first, E[] rest) {
      this.first = first;
      this.rest = checkNotNull(rest);
    }
    @Override public int size() {
      return rest.length + 1;
    }
    @Override public E get(int index) {
      // check explicitly so the IOOBE will have the right message
      checkElementIndex(index, size());
      return (index == 0) ? first : rest[index - 1];
    }
    private static final long serialVersionUID = 0;
  }

  /**
   * Returns an unmodifiable list containing the specified first and second
   * element, and backed by the specified array of additional elements. Changes
   * to the {@code rest} array will be reflected in the returned list. Unlike
   * {@link Arrays#asList}, the returned list is unmodifiable.
   *
   * <p>This is useful when a varargs method needs to use a signature such as
   * {@code (Foo firstFoo, Foo secondFoo, Foo... moreFoos)}, in order to avoid
   * overload ambiguity or to enforce a minimum argument count.
   *
   * <p>The returned list is serializable and implements {@link RandomAccess}.
   *
   * @param first the first element
   * @param second the second element
   * @param rest an array of additional elements, possibly empty
   * @return an unmodifiable list containing the specified elements
   */
  public static <E> List<E> asList(
      @Nullable E first, @Nullable E second, E[] rest) {
    return new TwoPlusArrayList<E>(first, second, rest);
  }

  /** @see Lists#asList(Object, Object, Object[]) */
  private static class TwoPlusArrayList<E> extends AbstractList<E>
      implements Serializable, RandomAccess {
    final E first;
    final E second;
    final E[] rest;

    TwoPlusArrayList(@Nullable E first, @Nullable E second, E[] rest) {
      this.first = first;
      this.second = second;
      this.rest = checkNotNull(rest);
    }
    @Override public int size() {
      return rest.length + 2;
    }
    @Override public E get(int index) {
      switch (index) {
        case 0:
          return first;
        case 1:
          return second;
        default:
          // check explicitly so the IOOBE will have the right message
          checkElementIndex(index, size());
          return rest[index - 2];
      }
    }
    private static final long serialVersionUID = 0;
  }

  /**
   * Returns a list that applies {@code function} to each element of {@code
   * fromList}. The returned list is a transformed view of {@code fromList};
   * changes to {@code fromList} will be reflected in the returned list and vice
   * versa.
   *
   * <p>Since functions are not reversible, the transform is one-way and new
   * items cannot be stored in the returned list. The {@code add},
   * {@code addAll} and {@code set} methods are unsupported in the returned
   * list.
   *
   * <p>The function is applied lazily, invoked when needed. This is necessary
   * for the returned list to be a view, but it means that the function will be
   * applied many times for bulk operations like {@link List#contains} and
   * {@link List#hashCode}. For this to perform well, {@code function} should be
   * fast. To avoid lazy evaluation when the returned list doesn't need to be a
   * view, copy the returned list into a new list of your choosing.
   *
   * <p>If {@code fromList} implements {@link RandomAccess}, so will the
   * returned list. The returned list always implements {@link Serializable},
   * but serialization will succeed only when {@code fromList} and
   * {@code function} are serializable. The returned list is threadsafe if the
   * supplied list and function are.
   */
  public static <F, T> List<T> transform(
      List<F> fromList, Function<? super F, ? extends T> function) {
    return (fromList instanceof RandomAccess)
        ? new TransformingRandomAccessList<F, T>(fromList, function)
        : new TransformingSequentialList<F, T>(fromList, function);
  }

  /**
   * Implementation of a sequential transforming list.
   *
   * @see Lists#transform
   */
  private static class TransformingSequentialList<F, T>
      extends AbstractSequentialList<T> implements Serializable {
    final List<F> fromList;
    final Function<? super F, ? extends T> function;

    TransformingSequentialList(
        List<F> fromList, Function<? super F, ? extends T> function) {
      this.fromList = checkNotNull(fromList);
      this.function = checkNotNull(function);
    }
    /**
     * The default implementation inherited is based on iteration and removal of
     * each element which can be overkill. That's why we forward this call
     * directly to the backing list.
     */
    @Override public void clear() {
      fromList.clear();
    }
    @Override public int size() {
      return fromList.size();
    }
    @Override public ListIterator<T> listIterator(final int index) {
      final ListIterator<F> delegate = fromList.listIterator(index);
      return new ListIterator<T>() {
        public void add(T e) {
          throw new UnsupportedOperationException();
        }

        public boolean hasNext() {
          return delegate.hasNext();
        }

        public boolean hasPrevious() {
          return delegate.hasPrevious();
        }

        public T next() {
          return function.apply(delegate.next());
        }

        public int nextIndex() {
          return delegate.nextIndex();
        }

        public T previous() {
          return function.apply(delegate.previous());
        }

        public int previousIndex() {
          return delegate.previousIndex();
        }

        public void remove() {
          delegate.remove();
        }

        public void set(T e) {
          throw new UnsupportedOperationException("not supported");
        }
      };
    }

    private static final long serialVersionUID = 0;
  }

  /**
   * Implementation of a transforming random access list. We try to make as many
   * of these methods pass-through to the source list as possible so that the
   * performance characteristics of the source list and transformed list are
   * similar.
   *
   * @see Lists#transform
   */
  private static class TransformingRandomAccessList<F, T>
      extends AbstractList<T> implements RandomAccess, Serializable {
    final List<F> fromList;
    final Function<? super F, ? extends T> function;

    TransformingRandomAccessList(
        List<F> fromList, Function<? super F, ? extends T> function) {
      this.fromList = checkNotNull(fromList);
      this.function = checkNotNull(function);
    }
    @Override public void clear() {
      fromList.clear();
    }
    @Override public T get(int index) {
      return function.apply(fromList.get(index));
    }
    @Override public boolean isEmpty() {
      return fromList.isEmpty();
    }
    @Override public T remove(int index) {
      return function.apply(fromList.remove(index));
    }
    @Override public int size() {
      return fromList.size();
    }
    private static final long serialVersionUID = 0;
  }

  /**
   * Returns consecutive {@linkplain List#subList(int, int) sublists} of a list,
   * each of the same size (the final list may be smaller). For example,
   * partitioning a list containing {@code [a, b, c, d, e]} with a partition
   * size of 3 yields {@code [[a, b, c], [d, e]]} -- an outer list containing
   * two inner lists of three and two elements, all in the original order.
   *
   * <p>The outer list is unmodifiable, but reflects the latest state of the
   * source list. The inner lists are sublist views of the original list,
   * produced on demand using {@link List#subList(int, int)}, and are subject
   * to all the usual caveats about modification as explained in that API.
   *
   * @param list the list to return consecutive sublists of
   * @param size the desired size of each sublist (the last may be
   *     smaller)
   * @return a list of consecutive sublists
   * @throws IllegalArgumentException if {@code partitionSize} is nonpositive
   */
  public static <T> List<List<T>> partition(List<T> list, int size) {
    checkNotNull(list);
    checkArgument(size > 0);
    return (list instanceof RandomAccess)
        ? new RandomAccessPartition<T>(list, size)
        : new Partition<T>(list, size);
  }

  private static class Partition<T> extends AbstractList<List<T>> {
    final List<T> list;
    final int size;

    Partition(List<T> list, int size) {
      this.list = list;
      this.size = size;
    }

    @Override public List<T> get(int index) {
      int listSize = size();
      checkElementIndex(index, listSize);
      int start = index * size;
      int end = Math.min(start + size, list.size());
      return Platform.subList(list, start, end);
    }

    @Override public int size() {
      return (list.size() + size - 1) / size;
    }

    @Override public boolean isEmpty() {
      return list.isEmpty();
    }
  }

  private static class RandomAccessPartition<T> extends Partition<T>
      implements RandomAccess {
    RandomAccessPartition(List<T> list, int size) {
      super(list, size);
    }
  }
}
