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
import org.elasticsearch.util.base.Objects;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * Provides static utility methods for creating and working with {@link
 * Multiset} instances.
 *
 * @author Kevin Bourrillion
 * @author Mike Bostock
 */
@GwtCompatible
public final class Multisets {
  private Multisets() {}

  /**
   * Returns an unmodifiable view of the specified multiset. Query operations on
   * the returned multiset "read through" to the specified multiset, and
   * attempts to modify the returned multiset result in an
   * {@link UnsupportedOperationException}.
   *
   * <p>The returned multiset will be serializable if the specified multiset is
   * serializable.
   *
   * @param multiset the multiset for which an unmodifiable view is to be
   *     generated
   * @return an unmodifiable view of the multiset
   */
  public static <E> Multiset<E> unmodifiableMultiset(
      Multiset<? extends E> multiset) {
    return new UnmodifiableMultiset<E>(multiset);
  }

  private static class UnmodifiableMultiset<E>
      extends ForwardingMultiset<E> implements Serializable {
    final Multiset<? extends E> delegate;

    UnmodifiableMultiset(Multiset<? extends E> delegate) {
      this.delegate = delegate;
    }

    @SuppressWarnings("unchecked")
    @Override protected Multiset<E> delegate() {
      // This is safe because all non-covariant methods are overriden
      return (Multiset) delegate;
    }

    transient Set<E> elementSet;

    @Override public Set<E> elementSet() {
      Set<E> es = elementSet;
      return (es == null)
          ? elementSet = Collections.<E>unmodifiableSet(delegate.elementSet())
          : es;
    }

    transient Set<Multiset.Entry<E>> entrySet;

    @SuppressWarnings("unchecked")
    @Override public Set<Multiset.Entry<E>> entrySet() {
      Set<Multiset.Entry<E>> es = entrySet;
      return (es == null)
          // Safe because the returned set is made unmodifiable and Entry
          // itself is readonly
          ? entrySet = (Set) Collections.unmodifiableSet(delegate.entrySet())
          : es;
    }

    @SuppressWarnings("unchecked")
    @Override public Iterator<E> iterator() {
      // Safe because the returned Iterator is made unmodifiable
      return (Iterator) Iterators.unmodifiableIterator(delegate.iterator());
    }

    @Override public boolean add(E element) {
      throw new UnsupportedOperationException();
    }

    @Override public int add(E element, int occurences) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(Collection<? extends E> elementsToAdd) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean remove(Object element) {
      throw new UnsupportedOperationException();
    }

    @Override public int remove(Object element, int occurrences) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean removeAll(Collection<?> elementsToRemove) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean retainAll(Collection<?> elementsToRetain) {
      throw new UnsupportedOperationException();
    }

    @Override public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override public int setCount(E element, int count) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean setCount(E element, int oldCount, int newCount) {
      throw new UnsupportedOperationException();
    }

    private static final long serialVersionUID = 0;
  }

  /**
   * Returns an immutable multiset entry with the specified element and count.
   *
   * @param e the element to be associated with the returned entry
   * @param n the count to be associated with the returned entry
   * @throws IllegalArgumentException if {@code n} is negative
   */
  public static <E> Multiset.Entry<E> immutableEntry(
      @Nullable final E e, final int n) {
    checkArgument(n >= 0);
    return new AbstractEntry<E>() {
      public E getElement() {
        return e;
      }
      public int getCount() {
        return n;
      }
    };
  }

  /**
   * Returns a multiset view of the specified set. The multiset is backed by the
   * set, so changes to the set are reflected in the multiset, and vice versa.
   * If the set is modified while an iteration over the multiset is in progress
   * (except through the iterator's own {@code remove} operation) the results of
   * the iteration are undefined.
   *
   * <p>The multiset supports element removal, which removes the corresponding
   * element from the set. It does not support the {@code add} or {@code addAll}
   * operations, nor does it support the use of {@code setCount} to add
   * elements.
   *
   * <p>The returned multiset will be serializable if the specified set is
   * serializable. The multiset is threadsafe if the set is threadsafe.
   *
   * @param set the backing set for the returned multiset view
   */
  static <E> Multiset<E> forSet(Set<E> set) {
    return new SetMultiset<E>(set);
  }

  /** @see Multisets#forSet */
  private static class SetMultiset<E> extends ForwardingCollection<E>
      implements Multiset<E>, Serializable {
    final Set<E> delegate;

    SetMultiset(Set<E> set) {
      delegate = checkNotNull(set);
    }

    @Override protected Set<E> delegate() {
      return delegate;
    }

    public int count(Object element) {
      return delegate.contains(element) ? 1 : 0;
    }

    public int add(E element, int occurrences) {
      throw new UnsupportedOperationException();
    }

    public int remove(Object element, int occurrences) {
      if (occurrences == 0) {
        return count(element);
      }
      checkArgument(occurrences > 0);
      return delegate.remove(element) ? 1 : 0;
    }

    transient Set<E> elementSet;

    public Set<E> elementSet() {
      Set<E> es = elementSet;
      return (es == null) ? elementSet = new ElementSet() : es;
    }

    transient Set<Entry<E>> entrySet;

    public Set<Entry<E>> entrySet() {
      Set<Entry<E>> es = entrySet;
      return (es == null) ? entrySet = new EntrySet() : es;
    }

    @Override public boolean add(E o) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException();
    }

    public int setCount(E element, int count) {
      checkNonnegative(count, "count");

      if (count == count(element)) {
        return count;
      } else if (count == 0) {
        remove(element);
        return 1;
      } else {
        throw new UnsupportedOperationException();
      }
    }

    public boolean setCount(E element, int oldCount, int newCount) {
      return setCountImpl(this, element, oldCount, newCount);
    }

    @Override public boolean equals(@Nullable Object object) {
      if (object instanceof Multiset) {
        Multiset<?> that = (Multiset<?>) object;
        return this.size() == that.size() && delegate.equals(that.elementSet());
      }
      return false;
    }

    @Override public int hashCode() {
      int sum = 0;
      for (E e : this) {
        sum += ((e == null) ? 0 : e.hashCode()) ^ 1;
      }
      return sum;
    }

    /** @see SetMultiset#elementSet */
    class ElementSet extends ForwardingSet<E> {
      @Override protected Set<E> delegate() {
        return delegate;
      }

      @Override public boolean add(E o) {
        throw new UnsupportedOperationException();
      }

      @Override public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
      }
    }

    /** @see SetMultiset#entrySet */
    class EntrySet extends AbstractSet<Entry<E>> {
      @Override public int size() {
        return delegate.size();
      }
      @Override public Iterator<Entry<E>> iterator() {
        return new Iterator<Entry<E>>() {
          final Iterator<E> elements = delegate.iterator();

          public boolean hasNext() {
            return elements.hasNext();
          }
          public Entry<E> next() {
            return immutableEntry(elements.next(), 1);
          }
          public void remove() {
            elements.remove();
          }
        };
      }
      // TODO: faster contains, remove
    }

    private static final long serialVersionUID = 0;
  }

  /**
   * Returns the expected number of distinct elements given the specified
   * elements. The number of distinct elements is only computed if {@code
   * elements} is an instance of {@code Multiset}; otherwise the default value
   * of 11 is returned.
   */
  static int inferDistinctElements(Iterable<?> elements) {
    if (elements instanceof Multiset) {
      return ((Multiset<?>) elements).elementSet().size();
    }
    return 11; // initial capacity will be rounded up to 16
  }

  /**
   * Implementation of the {@code equals}, {@code hashCode}, and
   * {@code toString} methods of {@link Multiset.Entry}.
   */
  abstract static class AbstractEntry<E> implements Multiset.Entry<E> {
    /**
     * Indicates whether an object equals this entry, following the behavior
     * specified in {@link Multiset.Entry#equals}.
     */
    @Override public boolean equals(@Nullable Object object) {
      if (object instanceof Multiset.Entry) {
        Multiset.Entry<?> that = (Multiset.Entry<?>) object;
        return this.getCount() == that.getCount()
            && Objects.equal(this.getElement(), that.getElement());
      }
      return false;
    }

    /**
     * Return this entry's hash code, following the behavior specified in
     * {@link Multiset.Entry#hashCode}.
     */
    @Override public int hashCode() {
      E e = getElement();
      return ((e == null) ? 0 : e.hashCode()) ^ getCount();
    }

    /**
     * Returns a string representation of this multiset entry. The string
     * representation consists of the associated element if the associated count
     * is one, and otherwise the associated element followed by the characters
     * " x " (space, x and space) followed by the count. Elements and counts are
     * converted to strings as by {@code String.valueOf}.
     */
    @Override public String toString() {
      String text = String.valueOf(getElement());
      int n = getCount();
      return (n == 1) ? text : (text + " x " + n);
    }
  }

  static <E> int setCountImpl(Multiset<E> self, E element, int count) {
    checkNonnegative(count, "count");

    int oldCount = self.count(element);

    int delta = count - oldCount;
    if (delta > 0) {
      self.add(element, delta);
    } else if (delta < 0) {
      self.remove(element, -delta);
    }

    return oldCount;
  }

  static <E> boolean setCountImpl(
      Multiset<E> self, E element, int oldCount, int newCount) {
    checkNonnegative(oldCount, "oldCount");
    checkNonnegative(newCount, "newCount");

    if (self.count(element) == oldCount) {
      self.setCount(element, newCount);
      return true;
    } else {
      return false;
    }
  }

  static void checkNonnegative(int count, String name) {
    checkArgument(count >= 0, "%s cannot be negative: %s", name, count);
  }
}
