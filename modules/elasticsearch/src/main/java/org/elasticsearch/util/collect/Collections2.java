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
import org.elasticsearch.util.base.Function;
import org.elasticsearch.util.base.Joiner;
import org.elasticsearch.util.base.Predicate;
import org.elasticsearch.util.base.Predicates;

import javax.annotation.Nullable;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * Provides static methods for working with {@code Collection} instances.
 *
 * @author Chris Povirk
 * @author Mike Bostock
 * @author Jared Levy
 */
@GwtCompatible
public final class Collections2 {
  private Collections2() {}

  /**
   * Returns {@code true} if the collection {@code self} contains all of the
   * elements in the collection {@code c}.
   *
   * <p>This method iterates over the specified collection {@code c}, checking
   * each element returned by the iterator in turn to see if it is contained in
   * the specified collection {@code self}. If all elements are so contained,
   * {@code true} is returned, otherwise {@code false}.
   *
   * @param self a collection which might contain all elements in {@code c}
   * @param c a collection whose elements might be contained by {@code self}
   */
  // TODO: Make public?
  static boolean containsAll(Collection<?> self, Collection<?> c) {
    checkNotNull(self);
    for (Object o : c) {
      if (!self.contains(o)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Converts an iterable into a collection. If the iterable is already a
   * collection, it is returned. Otherwise, an {@link java.util.ArrayList} is
   * created with the contents of the iterable in the same iteration order.
   */
  static <E> Collection<E> toCollection(Iterable<E> iterable) {
    return (iterable instanceof Collection)
        ? (Collection<E>) iterable : Lists.newArrayList(iterable);
  }

  /**
   * Returns the elements of {@code unfiltered} that satisfy a predicate. The
   * returned collection is a live view of {@code unfiltered}; changes to one
   * affect the other.
   *
   * <p>The resulting collection's iterator does not support {@code remove()},
   * but all other collection methods are supported. The collection's
   * {@code add()} and {@code addAll()} methods throw an
   * {@link IllegalArgumentException} if an element that doesn't satisfy the
   * predicate is provided. When methods such as {@code removeAll()} and
   * {@code clear()} are called on the filtered collection, only elements that
   * satisfy the filter will be removed from the underlying collection.
   *
   * <p>The returned collection isn't threadsafe or serializable, even if
   * {@code unfiltered} is.
   *
   * <p>Many of the filtered collection's methods, such as {@code size()},
   * iterate across every element in the underlying collection and determine
   * which elements satisfy the filter. When a live view is <i>not</i> needed,
   * it may be faster to copy {@code Iterables.filter(unfiltered, predicate)}
   * and use the copy.
   */
  public static <E> Collection<E> filter(
      Collection<E> unfiltered, Predicate<? super E> predicate) {
    if (unfiltered instanceof FilteredCollection) {
      // Support clear(), removeAll(), and retainAll() when filtering a filtered
      // collection.
      return ((FilteredCollection<E>) unfiltered).createCombined(predicate);
    }

    return new FilteredCollection<E>(
        checkNotNull(unfiltered), checkNotNull(predicate));
  }

  static class FilteredCollection<E> implements Collection<E> {
    final Collection<E> unfiltered;
    final Predicate<? super E> predicate;

    FilteredCollection(Collection<E> unfiltered,
        Predicate<? super E> predicate) {
      this.unfiltered = unfiltered;
      this.predicate = predicate;
    }

    FilteredCollection<E> createCombined(Predicate<? super E> newPredicate) {
      return new FilteredCollection<E>(unfiltered,
          Predicates.<E>and(predicate, newPredicate));
      // .<E> above needed to compile in JDK 5
    }

    public boolean add(E element) {
      checkArgument(predicate.apply(element));
      return unfiltered.add(element);
    }

    public boolean addAll(Collection<? extends E> collection) {
      for (E element : collection) {
        checkArgument(predicate.apply(element));
      }
      return unfiltered.addAll(collection);
    }

    public void clear() {
      Iterables.removeIf(unfiltered, predicate);
    }

    public boolean contains(Object element) {
      try {
        // unsafe cast can result in a CCE from predicate.apply(), which we
        // will catch
        @SuppressWarnings("unchecked")
        E e = (E) element;
        return predicate.apply(e) && unfiltered.contains(element);
      } catch (NullPointerException e) {
        return false;
      } catch (ClassCastException e) {
        return false;
      }
    }

    public boolean containsAll(Collection<?> collection) {
      for (Object element : collection) {
        if (!contains(element)) {
          return false;
        }
      }
      return true;
    }

    public boolean isEmpty() {
      return !Iterators.any(unfiltered.iterator(), predicate);
    }

    public Iterator<E> iterator() {
      return Iterators.filter(unfiltered.iterator(), predicate);
    }

    public boolean remove(Object element) {
      try {
        // unsafe cast can result in a CCE from predicate.apply(), which we
        // will catch
        @SuppressWarnings("unchecked")
        E e = (E) element;
        return predicate.apply(e) && unfiltered.remove(element);
      } catch (NullPointerException e) {
        return false;
      } catch (ClassCastException e) {
        return false;
      }
    }

    public boolean removeAll(final Collection<?> collection) {
      checkNotNull(collection);
      Predicate<E> combinedPredicate = new Predicate<E>() {
        public boolean apply(E input) {
          return predicate.apply(input) && collection.contains(input);
        }
      };
      return Iterables.removeIf(unfiltered, combinedPredicate);
    }

    public boolean retainAll(final Collection<?> collection) {
      checkNotNull(collection);
      Predicate<E> combinedPredicate = new Predicate<E>() {
        public boolean apply(E input) {
          return predicate.apply(input) && !collection.contains(input);
        }
      };
      return Iterables.removeIf(unfiltered, combinedPredicate);
    }

    public int size() {
      return Iterators.size(iterator());
    }

    public Object[] toArray() {
      // creating an ArrayList so filtering happens once
      return Lists.newArrayList(iterator()).toArray();
    }

    public <T> T[] toArray(T[] array) {
      return Lists.newArrayList(iterator()).toArray(array);
    }

    @Override public String toString() {
      return Iterators.toString(iterator());
    }
  }

  /**
   * Returns a collection that applies {@code function} to each element of
   * {@code fromCollection}. The returned collection is a live view of {@code
   * fromCollection}; changes to one affect the other.
   *
   * <p>The returned collection's {@code add()} and {@code addAll()} methods
   * throw an {@link UnsupportedOperationException}. All other collection
   * methods are supported, as long as {@code fromCollection} supports them.
   *
   * <p>The returned collection isn't threadsafe or serializable, even if
   * {@code fromCollection} is.
   *
   * <p>When a live view is <i>not</i> needed, it may be faster to copy the
   * transformed collection and use the copy.
   */
  public static <F, T> Collection<T> transform(Collection<F> fromCollection,
      Function<? super F, T> function) {
    return new TransformedCollection<F, T>(fromCollection, function);
  }

  static class TransformedCollection<F, T> extends AbstractCollection<T> {
    final Collection<F> fromCollection;
    final Function<? super F, ? extends T> function;

    TransformedCollection(Collection<F> fromCollection,
        Function<? super F, ? extends T> function) {
      this.fromCollection = checkNotNull(fromCollection);
      this.function = checkNotNull(function);
    }

    @Override public void clear() {
      fromCollection.clear();
    }

    @Override public boolean isEmpty() {
      return fromCollection.isEmpty();
    }

    @Override public Iterator<T> iterator() {
      return Iterators.transform(fromCollection.iterator(), function);
    }

    @Override public int size() {
      return fromCollection.size();
    }
  }

  static boolean setEquals(Set<?> thisSet, @Nullable Object object) {
    if (object == thisSet) {
      return true;
    }
    if (object instanceof Set) {
      Set<?> thatSet = (Set<?>) object;
      return thisSet.size() == thatSet.size()
          && thisSet.containsAll(thatSet);
    }
    return false;
  }

  static final Joiner standardJoiner = Joiner.on(", ");
}
