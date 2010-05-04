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
import org.elasticsearch.util.base.Function;
import org.elasticsearch.util.base.Objects;
import org.elasticsearch.util.base.Preconditions;
import org.elasticsearch.util.base.Predicate;

import javax.annotation.Nullable;
import java.util.*;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * This class contains static utility methods that operate on or return objects
 * of type {@code Iterable}. Except as noted, each method has a corresponding
 * {@link Iterator}-based method in the {@link Iterators} class.
 *
 * @author Kevin Bourrillion
 * @author Jared Levy
 */
@GwtCompatible
public final class Iterables {
  private Iterables() {}

  /** Returns an unmodifiable view of {@code iterable}. */
  public static <T> Iterable<T> unmodifiableIterable(final Iterable<T> iterable)
  {
    checkNotNull(iterable);
    return new Iterable<T>() {
      public Iterator<T> iterator() {
        return Iterators.unmodifiableIterator(iterable.iterator());
      }
      @Override public String toString() {
        return iterable.toString();
      }
      // no equals and hashCode; it would break the contract!
    };
  }

  /**
   * Returns the number of elements in {@code iterable}.
   */
  public static int size(Iterable<?> iterable) {
    return (iterable instanceof Collection)
        ? ((Collection<?>) iterable).size()
        : Iterators.size(iterable.iterator());
  }

  /**
   * Returns {@code true} if {@code iterable} contains {@code element}; that is,
   * any object for while {@code equals(element)} is true.
   */
  public static boolean contains(Iterable<?> iterable, @Nullable Object element)
  {
    if (iterable instanceof Collection) {
      Collection<?> collection = (Collection<?>) iterable;
      try {
        return collection.contains(element);
      } catch (NullPointerException e) {
        return false;
      } catch (ClassCastException e) {
        return false;
      }
    }
    return Iterators.contains(iterable.iterator(), element);
  }

  /**
   * Removes, from an iterable, every element that belongs to the provided
   * collection.
   *
   * <p>This method calls {@link Collection#removeAll} if {@code iterable} is a
   * collection, and {@link Iterators#removeAll} otherwise.
   *
   * @param removeFrom the iterable to (potentially) remove elements from
   * @param elementsToRemove the elements to remove
   * @return {@code true} if any elements are removed from {@code iterable}
   */
  public static boolean removeAll(
      Iterable<?> removeFrom, Collection<?> elementsToRemove) {
    return (removeFrom instanceof Collection)
        ? ((Collection<?>) removeFrom).removeAll(checkNotNull(elementsToRemove))
        : Iterators.removeAll(removeFrom.iterator(), elementsToRemove);
  }

  /**
   * Removes, from an iterable, every element that does not belong to the
   * provided collection.
   *
   * <p>This method calls {@link Collection#retainAll} if {@code iterable} is a
   * collection, and {@link Iterators#retainAll} otherwise.
   *
   * @param removeFrom the iterable to (potentially) remove elements from
   * @param elementsToRetain the elements to retain
   * @return {@code true} if any elements are removed from {@code iterable}
   */
  public static boolean retainAll(
      Iterable<?> removeFrom, Collection<?> elementsToRetain) {
    return (removeFrom instanceof Collection)
        ? ((Collection<?>) removeFrom).retainAll(checkNotNull(elementsToRetain))
        : Iterators.retainAll(removeFrom.iterator(), elementsToRetain);
  }

  /**
   * Removes, from an iterable, every element that satisfies the provided
   * predicate.
   *
   * @param removeFrom the iterable to (potentially) remove elements from
   * @param predicate a predicate that determines whether an element should
   *     be removed
   * @return {@code true} if any elements were removed from the iterable
   *
   * @throws UnsupportedOperationException if the iterable does not support
   *     {@code remove()}.
   */
  static <T> boolean removeIf(
      Iterable<T> removeFrom, Predicate<? super T> predicate) {
    if (removeFrom instanceof RandomAccess && removeFrom instanceof List) {
      return removeIfFromRandomAccessList(
          (List<T>) removeFrom, checkNotNull(predicate));
    }
    return Iterators.removeIf(removeFrom.iterator(), predicate);
  }

  private static <T> boolean removeIfFromRandomAccessList(
      List<T> list, Predicate<? super T> predicate) {
    int from = 0;
    int to = 0;

    for (; from < list.size(); from++) {
      T element = list.get(from);
      if (!predicate.apply(element)) {
        if (from > to) {
          list.set(to, element);
        }
        to++;
      }
    }

    // Clear the tail of any remaining items
    // Note: hand-written GWT-compatible version of
    // list.subList(to, list.size()).clear();
    ListIterator<T> iter = list.listIterator(list.size());
    for (int idx = from - to; idx > 0; idx--) {
      iter.previous();
      iter.remove();
    }

    return from != to;
  }

  /**
   * Determines whether two iterables contain equal elements in the same order.
   * More specifically, this method returns {@code true} if {@code iterable1}
   * and {@code iterable2} contain the same number of elements and every element
   * of {@code iterable1} is equal to the corresponding element of
   * {@code iterable2}.
   */
  public static boolean elementsEqual(
      Iterable<?> iterable1, Iterable<?> iterable2) {
    return Iterators.elementsEqual(iterable1.iterator(), iterable2.iterator());
  }

  /**
   * Returns a string representation of {@code iterable}, with the format
   * {@code [e1, e2, ..., en]}.
   */
  public static String toString(Iterable<?> iterable) {
    return Iterators.toString(iterable.iterator());
  }

  /**
   * Returns the single element contained in {@code iterable}.
   *
   * @throws NoSuchElementException if the iterable is empty
   * @throws IllegalArgumentException if the iterable contains multiple
   *     elements
   */
  public static <T> T getOnlyElement(Iterable<T> iterable) {
    return Iterators.getOnlyElement(iterable.iterator());
  }

  /**
   * Returns the single element contained in {@code iterable}, or {@code
   * defaultValue} if the iterable is empty.
   *
   * @throws IllegalArgumentException if the iterator contains multiple
   *     elements
   */
  public static <T> T getOnlyElement(
      Iterable<T> iterable, @Nullable T defaultValue) {
    return Iterators.getOnlyElement(iterable.iterator(), defaultValue);
  }

  /**
   * Copies an iterable's elements into an array.
   *
   * @param iterable the iterable to copy
   * @param type the type of the elements
   * @return a newly-allocated array into which all the elements of the iterable
   *     have been copied
   */
  @GwtIncompatible("Array.newInstance(Class, int)")
  public static <T> T[] toArray(Iterable<? extends T> iterable, Class<T> type) {
    @SuppressWarnings("unchecked") // bugs.sun.com/view_bug.do?bug_id=6558557
    Collection<? extends T> collection = (iterable instanceof Collection)
        ? (Collection<? extends T>) iterable
        : Lists.newArrayList(iterable);
    T[] array = ObjectArrays.newArray(type, collection.size());
    return collection.toArray(array);
  }

  /**
   * Adds all elements in {@code iterable} to {@code collection}.
   *
   * @return {@code true} if {@code collection} was modified as a result of this
   *     operation.
   */
  public static <T> boolean addAll(
      Collection<T> addTo, Iterable<? extends T> elementsToAdd) {
    if (elementsToAdd instanceof Collection) {
      @SuppressWarnings("unchecked")
      Collection<? extends T> c = (Collection<? extends T>) elementsToAdd;
      return addTo.addAll(c);
    }
    return Iterators.addAll(addTo, elementsToAdd.iterator());
  }

  /**
   * Returns the number of elements in the specified iterable that equal the
   * specified object.
   *
   * @see Collections#frequency
   */
  public static int frequency(Iterable<?> iterable, @Nullable Object element) {
    if ((iterable instanceof Multiset)) {
      return ((Multiset<?>) iterable).count(element);
    }
    if ((iterable instanceof Set)) {
      return ((Set<?>) iterable).contains(element) ? 1 : 0;
    }
    return Iterators.frequency(iterable.iterator(), element);
  }

  /**
   * Returns an iterable whose iterators cycle indefinitely over the elements of
   * {@code iterable}.
   *
   * <p>That iterator supports {@code remove()} if {@code iterable.iterator()}
   * does. After {@code remove()} is called, subsequent cycles omit the removed
   * element, which is no longer in {@code iterable}. The iterator's
   * {@code hasNext()} method returns {@code true} until {@code iterable} is
   * empty.
   *
   * <p><b>Warning:</b> Typical uses of the resulting iterator may produce an
   * infinite loop. You should use an explicit {@code break} or be certain that
   * you will eventually remove all the elements.
   *
   * <p>To cycle over the iterable {@code n} times, use the following:
   * {@code Iterables.concat(Collections.nCopies(n, iterable))}
   */
  public static <T> Iterable<T> cycle(final Iterable<T> iterable) {
    checkNotNull(iterable);
    return new Iterable<T>() {
      public Iterator<T> iterator() {
        return Iterators.cycle(iterable);
      }
      @Override public String toString() {
        return iterable.toString() + " (cycled)";
      }
    };
  }

  /**
   * Returns an iterable whose iterators cycle indefinitely over the provided
   * elements.
   *
   * <p>After {@code remove} is invoked on a generated iterator, the removed
   * element will no longer appear in either that iterator or any other iterator
   * created from the same source iterable. That is, this method behaves exactly
   * as {@code Iterables.cycle(Lists.newArrayList(elements))}. The iterator's
   * {@code hasNext} method returns {@code true} until all of the original
   * elements have been removed.
   *
   * <p><b>Warning:</b> Typical uses of the resulting iterator may produce an
   * infinite loop. You should use an explicit {@code break} or be certain that
   * you will eventually remove all the elements.
   *
   * <p>To cycle over the elements {@code n} times, use the following:
   * {@code Iterables.concat(Collections.nCopies(n, Arrays.asList(elements)))}
   */
  public static <T> Iterable<T> cycle(T... elements) {
    return cycle(Lists.newArrayList(elements));
  }

  /**
   * Combines two iterables into a single iterable. The returned iterable has an
   * iterator that traverses the elements in {@code a}, followed by the elements
   * in {@code b}. The source iterators are not polled until necessary.
   *
   * <p>The returned iterable's iterator supports {@code remove()} when the
   * corresponding input iterator supports it.
   */
  @SuppressWarnings("unchecked")
  public static <T> Iterable<T> concat(
      Iterable<? extends T> a, Iterable<? extends T> b) {
    checkNotNull(a);
    checkNotNull(b);
    return concat(Arrays.asList(a, b));
  }

  /**
   * Combines three iterables into a single iterable. The returned iterable has
   * an iterator that traverses the elements in {@code a}, followed by the
   * elements in {@code b}, followed by the elements in {@code c}. The source
   * iterators are not polled until necessary.
   *
   * <p>The returned iterable's iterator supports {@code remove()} when the
   * corresponding input iterator supports it.
   */
  @SuppressWarnings("unchecked")
  public static <T> Iterable<T> concat(Iterable<? extends T> a,
      Iterable<? extends T> b, Iterable<? extends T> c) {
    checkNotNull(a);
    checkNotNull(b);
    checkNotNull(c);
    return concat(Arrays.asList(a, b, c));
  }

  /**
   * Combines four iterables into a single iterable. The returned iterable has
   * an iterator that traverses the elements in {@code a}, followed by the
   * elements in {@code b}, followed by the elements in {@code c}, followed by
   * the elements in {@code d}. The source iterators are not polled until
   * necessary.
   *
   * <p>The returned iterable's iterator supports {@code remove()} when the
   * corresponding input iterator supports it.
   */
  @SuppressWarnings("unchecked")
  public static <T> Iterable<T> concat(Iterable<? extends T> a,
      Iterable<? extends T> b, Iterable<? extends T> c,
      Iterable<? extends T> d) {
    checkNotNull(a);
    checkNotNull(b);
    checkNotNull(c);
    checkNotNull(d);
    return concat(Arrays.asList(a, b, c, d));
  }

  /**
   * Combines multiple iterables into a single iterable. The returned iterable
   * has an iterator that traverses the elements of each iterable in
   * {@code inputs}. The input iterators are not polled until necessary.
   *
   * <p>The returned iterable's iterator supports {@code remove()} when the
   * corresponding input iterator supports it.
   *
   * @throws NullPointerException if any of the provided iterables is null
   */
  public static <T> Iterable<T> concat(Iterable<? extends T>... inputs) {
    return concat(ImmutableList.of(inputs));
  }

  /**
   * Combines multiple iterables into a single iterable. The returned iterable
   * has an iterator that traverses the elements of each iterable in
   * {@code inputs}. The input iterators are not polled until necessary.
   *
   * <p>The returned iterable's iterator supports {@code remove()} when the
   * corresponding input iterator supports it. The methods of the returned
   * iterable may throw {@code NullPointerException} if any of the input
   * iterators are null.
   */
  public static <T> Iterable<T> concat(
      Iterable<? extends Iterable<? extends T>> inputs) {
    /*
     * Hint: if you let A represent Iterable<? extends T> and B represent
     * Iterator<? extends T>, then this Function would look simply like:
     *
     *   Function<A, B> function = new Function<A, B> {
     *     public B apply(A from) {
     *       return from.iterator();
     *     }
     *   }
     *
     * TODO: there may be a better way to do this.
     */

    Function<Iterable<? extends T>, Iterator<? extends T>> function
        = new Function<Iterable<? extends T>, Iterator<? extends T>>() {
      public Iterator<? extends T> apply(Iterable<? extends T> from) {
        return from.iterator();
      }
    };
    final Iterable<Iterator<? extends T>> iterators
        = transform(inputs, function);
    return new IterableWithToString<T>() {
      public Iterator<T> iterator() {
        return Iterators.concat(iterators.iterator());
      }
    };
  }

  /**
   * Divides an iterable into unmodifiable sublists of the given size (the final
   * iterable may be smaller). For example, partitioning an iterable containing
   * {@code [a, b, c, d, e]} with a partition size of 3 yields {@code
   * [[a, b, c], [d, e]]} -- an outer iterable containing two inner lists of
   * three and two elements, all in the original order.
   *
   * <p>Iterators returned by the returned iterable do not support the {@link
   * Iterator#remove()} method. The returned lists implement {@link
   * RandomAccess}, whether or not the input list does.
   *
   * <p><b>Note:</b> if {@code iterable} is a {@link List}, use {@link
   * Lists#partition(List, int)} instead.
   *
   * @param iterable the iterable to return a partitioned view of
   * @param size the desired size of each partition (the last may be smaller)
   * @return an iterable of unmodifiable lists containing the elements of {@code
   *     iterable} divided into partitions
   * @throws IllegalArgumentException if {@code size} is nonpositive
   */
  public static <T> Iterable<List<T>> partition(
      final Iterable<T> iterable, final int size) {
    checkNotNull(iterable);
    checkArgument(size > 0);
    return new IterableWithToString<List<T>>() {
      public Iterator<List<T>> iterator() {
        return Iterators.partition(iterable.iterator(), size);
      }
    };
  }

  /**
   * Divides an iterable into unmodifiable sublists of the given size, padding
   * the final iterable with null values if necessary. For example, partitioning
   * an iterable containing {@code [a, b, c, d, e]} with a partition size of 3
   * yields {@code [[a, b, c], [d, e, null]]} -- an outer iterable containing
   * two inner lists of three elements each, all in the original order.
   *
   * <p>Iterators returned by the returned iterable do not support the {@link
   * Iterator#remove()} method.
   *
   * @param iterable the iterable to return a partitioned view of
   * @param size the desired size of each partition
   * @return an iterable of unmodifiable lists containing the elements of {@code
   *     iterable} divided into partitions (the final iterable may have
   *     trailing null elements)
   * @throws IllegalArgumentException if {@code size} is nonpositive
   */
  public static <T> Iterable<List<T>> paddedPartition(
      final Iterable<T> iterable, final int size) {
    checkNotNull(iterable);
    checkArgument(size > 0);
    return new IterableWithToString<List<T>>() {
      public Iterator<List<T>> iterator() {
        return Iterators.paddedPartition(iterable.iterator(), size);
      }
    };
  }

  /**
   * Returns the elements of {@code unfiltered} that satisfy a predicate. The
   * resulting iterable's iterator does not support {@code remove()}.
   */
  public static <T> Iterable<T> filter(
      final Iterable<T> unfiltered, final Predicate<? super T> predicate) {
    checkNotNull(unfiltered);
    checkNotNull(predicate);
    return new IterableWithToString<T>() {
      public Iterator<T> iterator() {
        return Iterators.filter(unfiltered.iterator(), predicate);
      }
    };
  }

  /**
   * Returns all instances of class {@code type} in {@code unfiltered}. The
   * returned iterable has elements whose class is {@code type} or a subclass of
   * {@code type}. The returned iterable's iterator does not support
   * {@code remove()}.
   *
   * @param unfiltered an iterable containing objects of any type
   * @param type the type of elements desired
   * @return an unmodifiable iterable containing all elements of the original
   *     iterable that were of the requested type
   */
  @GwtIncompatible("Class.isInstance")
  public static <T> Iterable<T> filter(
      final Iterable<?> unfiltered, final Class<T> type) {
    checkNotNull(unfiltered);
    checkNotNull(type);
    return new IterableWithToString<T>() {
      public Iterator<T> iterator() {
        return Iterators.filter(unfiltered.iterator(), type);
      }
    };
  }

  /**
   * Returns {@code true} if one or more elements in {@code iterable} satisfy
   * the predicate.
   */
  public static <T> boolean any(
      Iterable<T> iterable, Predicate<? super T> predicate) {
    return Iterators.any(iterable.iterator(), predicate);
  }

  /**
   * Returns {@code true} if every element in {@code iterable} satisfies the
   * predicate. If {@code iterable} is empty, {@code true} is returned.
   */
  public static <T> boolean all(
      Iterable<T> iterable, Predicate<? super T> predicate) {
    return Iterators.all(iterable.iterator(), predicate);
  }

  /**
   * Returns the first element in {@code iterable} that satisfies the given
   * predicate.
   *
   * @throws NoSuchElementException if no element in {@code iterable} matches
   *     the given predicate
   */
  public static <T> T find(Iterable<T> iterable,
      Predicate<? super T> predicate) {
    return Iterators.find(iterable.iterator(), predicate);
  }

  /**
   * Returns an iterable that applies {@code function} to each element of {@code
   * fromIterable}.
   *
   * <p>The returned iterable's iterator supports {@code remove()} if the
   * provided iterator does. After a successful {@code remove()} call,
   * {@code fromIterable} no longer contains the corresponding element.
   */
  public static <F, T> Iterable<T> transform(final Iterable<F> fromIterable,
      final Function<? super F, ? extends T> function) {
    checkNotNull(fromIterable);
    checkNotNull(function);
    return new IterableWithToString<T>() {
      public Iterator<T> iterator() {
        return Iterators.transform(fromIterable.iterator(), function);
      }
    };
  }

  /**
   * Returns the element at the specified position in an iterable.
   *
   * @param position position of the element to return
   * @return the element at the specified position in {@code iterable}
   * @throws IndexOutOfBoundsException if {@code position} is negative or
   *     greater than or equal to the size of {@code iterable}
   */
  public static <T> T get(Iterable<T> iterable, int position) {
    checkNotNull(iterable);
    if (iterable instanceof List) {
      return ((List<T>) iterable).get(position);
    }

    if (iterable instanceof Collection) {
      // Can check both ends
      Collection<T> collection = (Collection<T>) iterable;
      Preconditions.checkElementIndex(position, collection.size());
    } else {
      // Can only check the lower end
      if (position < 0) {
        throw new IndexOutOfBoundsException(
            "position cannot be negative: " + position);
      }
    }
    return Iterators.get(iterable.iterator(), position);
  }

  /**
   * Returns the last element of {@code iterable}.
   *
   * @return the last element of {@code iterable}
   * @throws NoSuchElementException if the iterable has no elements
   */
  public static <T> T getLast(Iterable<T> iterable) {
    if (iterable instanceof List) {
      List<T> list = (List<T>) iterable;
      // TODO: Support a concurrent list whose size changes while this method
      // is running.
      if (list.isEmpty()) {
        throw new NoSuchElementException();
      }
      return list.get(list.size() - 1);
    }

    if (iterable instanceof SortedSet) {
      SortedSet<T> sortedSet = (SortedSet<T>) iterable;
      return sortedSet.last();
    }

    return Iterators.getLast(iterable.iterator());
  }

  // Methods only in Iterables, not in Iterators

  /**
   * Adapts a list to an iterable with reversed iteration order. It is
   * especially useful in foreach-style loops: <pre class="code">   {@code
   *
   *   List<String> mylist = ...
   *   for (String str : Iterables.reverse(mylist)) {
   *     ...
   *   }}</pre>
   *
   * There is no corresponding method in {@link Iterators}, since {@link
   * Iterable#iterator} can simply be invoked on the result of calling this
   * method.
   *
   * @return an iterable with the same elements as the list, in reverse
   */
  public static <T> Iterable<T> reverse(final List<T> list) {
    checkNotNull(list);
    return new IterableWithToString<T>() {
      public Iterator<T> iterator() {
        final ListIterator<T> listIter = list.listIterator(list.size());
        return new Iterator<T>() {
          public boolean hasNext() {
            return listIter.hasPrevious();
          }
          public T next() {
            return listIter.previous();
          }
          public void remove() {
            listIter.remove();
          }
        };
      }
    };
  }

  /**
   * Determines if the given iterable contains no elements.
   *
   * <p>There is no precise {@link Iterator} equivalent to this method, since
   * one can only ask an iterator whether it has any elements <i>remaining</i>
   * (which one does using {@link Iterator#hasNext}).
   *
   * @return {@code true} if the iterable contains no elements
   */
  public static <T> boolean isEmpty(Iterable<T> iterable) {
    return !iterable.iterator().hasNext();
  }

  /**
   * Removes the specified element from the specified iterable.
   *
   * <p>This method iterates over the iterable, checking each element returned
   * by the iterator in turn to see if it equals the object {@code o}. If they
   * are equal, it is removed from the iterable with the iterator's
   * {@code remove} method. At most one element is removed, even if the iterable
   * contains multiple members that equal {@code o}.
   *
   * <p><b>Warning</b>: Do not use this method for a collection, such as a
   * {@link HashSet}, that has a fast {@code remove} method.
   *
   * @param iterable the iterable from which to remove
   * @param o an element to remove from the collection
   * @return {@code true} if the iterable changed as a result
   * @throws UnsupportedOperationException if the iterator does not support the
   *     {@code remove} method and the iterable contains the object
   */
  static boolean remove(Iterable<?> iterable, @Nullable Object o) {
    Iterator<?> i = iterable.iterator();
    while (i.hasNext()) {
      if (Objects.equal(i.next(), o)) {
        i.remove();
        return true;
      }
    }
    return false;
  }

  abstract static class IterableWithToString<E> implements Iterable<E> {
    @Override public String toString() {
      return Iterables.toString(this);
    }
  }
}
