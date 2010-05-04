/*
 * Copyright (C) 2009 Google Inc.
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
import java.util.*;

/**
 * An empty immutable sorted set with one or more elements.
 * TODO: Consider creating a separate class for a single-element sorted set.
 * 
 * @author Jared Levy
 */
@GwtCompatible(serializable = true)
@SuppressWarnings("serial")
final class RegularImmutableSortedSet<E>
    extends ImmutableSortedSet<E> {

  private final Object[] elements;
  /**
   * The index of the first element that's in the sorted set (inclusive
   * index).
   */
  private final int fromIndex;
  /**
   * The index after the last element that's in the sorted set (exclusive
   * index).
   */
  private final int toIndex;

  RegularImmutableSortedSet(Object[] elements,
      Comparator<? super E> comparator) {
    super(comparator);
    this.elements = elements;
    this.fromIndex = 0;
    this.toIndex = elements.length;
  }

  RegularImmutableSortedSet(Object[] elements,
      Comparator<? super E> comparator, int fromIndex, int toIndex) {
    super(comparator);
    this.elements = elements;
    this.fromIndex = fromIndex;
    this.toIndex = toIndex;
  }

  // The factory methods ensure that every element is an E.
  @SuppressWarnings("unchecked")
  @Override public UnmodifiableIterator<E> iterator() {
    return (UnmodifiableIterator<E>)
        Iterators.forArray(elements, fromIndex, size());
  }

  @Override public boolean isEmpty() {
    return false;
  }

  public int size() {
    return toIndex - fromIndex;
  }

  @Override public boolean contains(Object o) {
    if (o == null) {
      return false;
    }
    try {
      return binarySearch(o) >= 0;
    } catch (ClassCastException e) {
      return false;
    }
  }

  @Override public boolean containsAll(Collection<?> targets) {
    // TODO: For optimal performance, use a binary search when
    // targets.size() < size() / log(size())
    if (!hasSameComparator(targets, comparator()) || (targets.size() <= 1)) {
      return super.containsAll(targets);
    }

    /*
     * If targets is a sorted set with the same comparator, containsAll can
     * run in O(n) time stepping through the two collections.
     */
    int i = fromIndex;
    Iterator<?> iterator = targets.iterator();
    Object target = iterator.next();

    while (true) {
      if (i >= toIndex) {
        return false;
      }

      int cmp = unsafeCompare(elements[i], target);

      if (cmp < 0) {
        i++;
      } else if (cmp == 0) {
        if (!iterator.hasNext()) {
          return true;
        }
        target = iterator.next();
        i++;
      } else if (cmp > 0) {
        return false;
      }
    }
  }

  private int binarySearch(Object key) {
    int lower = fromIndex;
    int upper = toIndex - 1;

    while (lower <= upper) {
      int middle = lower + (upper - lower) / 2;
      int c = unsafeCompare(key, elements[middle]);
      if (c < 0) {
        upper = middle - 1;
      } else if (c > 0) {
        lower = middle + 1;
      } else {
        return middle;
      }
    }

    return -lower - 1;
  }

  @Override public Object[] toArray() {
    Object[] array = new Object[size()];
    System.arraycopy(elements, fromIndex, array, 0, size());
    return array;
  }

  // TODO: Move to ObjectArrays (same code in ImmutableList).
  @Override public <T> T[] toArray(T[] array) {
    int size = size();
    if (array.length < size) {
      array = ObjectArrays.newArray(array, size);
    } else if (array.length > size) {
      array[size] = null;
    }
    System.arraycopy(elements, fromIndex, array, 0, size);
    return array;
  }

  @Override public boolean equals(@Nullable Object object) {
    if (object == this) {
      return true;
    }
    if (!(object instanceof Set)) {
      return false;
    }

    Set<?> that = (Set<?>) object;
    if (size() != that.size()) {
      return false;
    }

    if (hasSameComparator(that, comparator)) {
      Iterator<?> iterator = that.iterator();
      try {
        for (int i = fromIndex; i < toIndex; i++) {
          Object otherElement = iterator.next();
          if (otherElement == null
              || unsafeCompare(elements[i], otherElement) != 0) {
            return false;
          }
        }
        return true;
      } catch (ClassCastException e) {
        return false;
      } catch (NoSuchElementException e) {
        return false; // concurrent change to other set
      }
    }
    return this.containsAll(that);
  }

  @Override public int hashCode() {
    // not caching hash code since it could change if the elements are mutable
    // in a way that modifies their hash codes
    int hash = 0;
    for (int i = fromIndex; i < toIndex; i++) {
      hash += elements[i].hashCode();
    }
    return hash;
  }

  // The factory methods ensure that every element is an E.
  @SuppressWarnings("unchecked")
  public E first() {
    return (E) elements[fromIndex];
  }

  // The factory methods ensure that every element is an E.
  @SuppressWarnings("unchecked")
  public E last() {
    return (E) elements[toIndex - 1];
  }

  @Override ImmutableSortedSet<E> headSetImpl(E toElement) {
    return createSubset(fromIndex, findSubsetIndex(toElement));
  }

  @Override ImmutableSortedSet<E> subSetImpl(E fromElement, E toElement) {
    return createSubset(
        findSubsetIndex(fromElement), findSubsetIndex(toElement));
  }

  @Override ImmutableSortedSet<E> tailSetImpl(E fromElement) {
    return createSubset(findSubsetIndex(fromElement), toIndex);
  }

  private int findSubsetIndex(E element) {
    int index = binarySearch(element);
    return (index >= 0) ? index : (-index - 1);
  }

  private ImmutableSortedSet<E> createSubset(
      int newFromIndex, int newToIndex) {
    if (newFromIndex < newToIndex) {
      return new RegularImmutableSortedSet<E>(elements, comparator,
          newFromIndex, newToIndex);
    } else {
      return emptySet(comparator);
    }
  }

  @Override boolean hasPartialArray() {
    return (fromIndex != 0) || (toIndex != elements.length);
  }
}
