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

import org.elasticsearch.util.annotations.VisibleForTesting;
import org.elasticsearch.util.collect.Serialization.FieldSetter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.util.base.Preconditions.*;
import static org.elasticsearch.util.collect.Multisets.*;

/**
 * A multiset that supports concurrent modifications and that provides atomic
 * versions of most {@code Multiset} operations (exceptions where noted). Null
 * elements are not supported.
 *
 * @author Cliff L. Biffle
 */
public final class ConcurrentHashMultiset<E> extends AbstractMultiset<E>
    implements Serializable {
  /*
   * The ConcurrentHashMultiset's atomic operations are implemented in terms of
   * ConcurrentMap's atomic operations. Many of them, such as add(E, int), are
   * read-modify-write sequences, and so are implemented as loops that wrap
   * ConcurrentMap's compare-and-set operations (like putIfAbsent).
   */

  /** The number of occurrences of each element. */
  private final transient ConcurrentMap<E, Integer> countMap;

  // This constant allows the deserialization code to set a final field. This
  // holder class makes sure it is not initialized unless an instance is
  // deserialized.
  private static class FieldSettersHolder {
    @SuppressWarnings("unchecked")
    // eclipse doesn't like the raw type here, but it's harmless
    static final FieldSetter<ConcurrentHashMultiset> COUNT_MAP_FIELD_SETTER
        = Serialization.getFieldSetter(
            ConcurrentHashMultiset.class, "countMap");
  }

  /**
   * Creates a new, empty {@code ConcurrentHashMultiset} using the default
   * initial capacity, load factor, and concurrency settings.
   */
  public static <E> ConcurrentHashMultiset<E> create() {
    return new ConcurrentHashMultiset<E>(new ConcurrentHashMap<E, Integer>());
  }

  /**
   * Creates a new {@code ConcurrentHashMultiset} containing the specified
   * elements, using the default initial capacity, load factor, and concurrency
   * settings.
   *
   * @param elements the elements that the multiset should contain
   */
  public static <E> ConcurrentHashMultiset<E> create(
      Iterable<? extends E> elements) {
    ConcurrentHashMultiset<E> multiset = ConcurrentHashMultiset.create();
    Iterables.addAll(multiset, elements);
    return multiset;
  }

  /**
   * Creates an instance using {@code countMap} to store elements and their
   * counts.
   *
   * <p>This instance will assume ownership of {@code countMap}, and other code
   * should not maintain references to the map or modify it in any way.
   *
   * @param countMap backing map for storing the elements in the multiset and
   *     their counts. It must be empty.
   * @throws IllegalArgumentException if {@code countMap} is not empty
   */
  @VisibleForTesting ConcurrentHashMultiset(
      ConcurrentMap<E, Integer> countMap) {
    checkArgument(countMap.isEmpty());
    this.countMap = countMap;
  }

  // Query Operations

  /**
   * Returns the number of occurrences of {@code element} in this multiset.
   *
   * @param element the element to look for
   * @return the nonnegative number of occurrences of the element
   */
  @Override public int count(@Nullable Object element) {
    try {
      return unbox(countMap.get(element));
    } catch (NullPointerException e) {
      return 0;
    } catch (ClassCastException e) {
      return 0;
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>If the data in the multiset is modified by any other threads during this
   * method, it is undefined which (if any) of these modifications will be
   * reflected in the result.
   */
  @Override public int size() {
    long sum = 0L;
    for (Integer value : countMap.values()) {
      sum += value;
    }
    return (int) Math.min(sum, Integer.MAX_VALUE);
  }

  /*
   * Note: the superclass toArray() methods assume that size() gives a correct
   * answer, which ours does not.
   */

  @Override public Object[] toArray() {
    return snapshot().toArray();
  }

  @Override public <T> T[] toArray(T[] array) {
    return snapshot().toArray(array);
  }

  /*
   * We'd love to use 'new ArrayList(this)' or 'list.addAll(this)', but
   * either of these would recurse back to us again!
   */
  private List<E> snapshot() {
    List<E> list = Lists.newArrayListWithExpectedSize(size());
    for (Multiset.Entry<E> entry : entrySet()) {
      E element = entry.getElement();
      for (int i = entry.getCount(); i > 0; i--) {
        list.add(element);
      }
    }
    return list;
  }

  // Modification Operations

  /**
   * Adds a number of occurrences of the specified element to this multiset.
   *
   * @param element the element to add
   * @param occurrences the number of occurrences to add
   * @return the previous count of the element before the operation; possibly
   *     zero
   * @throws IllegalArgumentException if {@code occurrences} is negative, or if
   *     the resulting amount would exceed {@link Integer#MAX_VALUE}
   */
  @Override public int add(E element, int occurrences) {
    if (occurrences == 0) {
      return count(element);
    }
    checkArgument(occurrences > 0, "Invalid occurrences: %s", occurrences);

    while (true) {
      int current = count(element);
      if (current == 0) {
        if (countMap.putIfAbsent(element, occurrences) == null) {
          return 0;
        }
      } else {
        checkArgument(occurrences <= Integer.MAX_VALUE - current,
            "Overflow adding %s occurrences to a count of %s",
            occurrences, current);
        int next = current + occurrences;
        if (countMap.replace(element, current, next)) {
          return current;
        }
      }
      // If we're still here, there was a race, so just try again.
    }
  }

  /**
   * Removes a number of occurrences of the specified element from this
   * multiset. If the multiset contains fewer than this number of occurrences to
   * begin with, all occurrences will be removed.
   *
   * @param element the element whose occurrences should be removed
   * @param occurrences the number of occurrences of the element to remove
   * @return the count of the element before the operation; possibly zero
   * @throws IllegalArgumentException if {@code occurrences} is negative
   */
  @Override public int remove(@Nullable Object element, int occurrences) {
    if (occurrences == 0) {
      return count(element);
    }
    checkArgument(occurrences > 0, "Invalid occurrences: %s", occurrences);

    while (true) {
      int current = count(element);
      if (current == 0) {
        return 0;
      }
      if (occurrences >= current) {
        if (countMap.remove(element, current)) {
          return current;
        }
      } else {
        // We know it's an "E" because it already exists in the map.
        @SuppressWarnings("unchecked")
        E casted = (E) element;

        if (countMap.replace(casted, current, current - occurrences)) {
          return current;
        }
      }
      // If we're still here, there was a race, so just try again.
    }
  }

  /**
   * Removes <b>all</b> occurrences of the specified element from this multiset.
   * This method complements {@link Multiset#remove(Object)}, which removes only
   * one occurrence at a time.
   *
   * @param element the element whose occurrences should all be removed
   * @return the number of occurrences successfully removed, possibly zero
   */
  private int removeAllOccurrences(@Nullable Object element) {
    try {
      return unbox(countMap.remove(element));
    } catch (NullPointerException e) {
      return 0;
    } catch (ClassCastException e) {
      return 0;
    }
  }

  /**
   * Removes exactly the specified number of occurrences of {@code element}, or
   * makes no change if this is not possible.
   *
   * <p>This method, in contrast to {@link #remove(Object, int)}, has no effect
   * when the element count is smaller than {@code occurrences}.
   *
   * @param element the element to remove
   * @param occurrences the number of occurrences of {@code element} to remove
   * @return {@code true} if the removal was possible (including if {@code
   *     occurrences} is zero)
   */
  public boolean removeExactly(@Nullable Object element, int occurrences) {
    if (occurrences == 0) {
      return true;
    }
    checkArgument(occurrences > 0, "Invalid occurrences: %s", occurrences);

    while (true) {
      int current = count(element);
      if (occurrences > current) {
        return false;
      }
      if (occurrences == current) {
        if (countMap.remove(element, occurrences)) {
          return true;
        }
      } else {
        @SuppressWarnings("unchecked") // it's in the map, must be an "E"
        E casted = (E) element;
        if (countMap.replace(casted, current, current - occurrences)) {
          return true;
        }
      }
      // If we're still here, there was a race, so just try again.
    }
  }

  /**
   * Adds or removes occurrences of {@code element} such that the {@link #count}
   * of the element becomes {@code count}.
   *
   * @return the count of {@code element} in the multiset before this call
   * @throws IllegalArgumentException if {@code count} is negative
   */
  @Override public int setCount(E element, int count) {
    checkNonnegative(count, "count");
    return (count == 0)
        ? removeAllOccurrences(element)
        : unbox(countMap.put(element, count));
  }

  /**
   * Sets the number of occurrences of {@code element} to {@code newCount}, but
   * only if the count is currently {@code oldCount}. If {@code element} does
   * not appear in the multiset exactly {@code oldCount} times, no changes will
   * be made.
   *
   * @return {@code true} if the change was successful. This usually indicates
   *     that the multiset has been modified, but not always: in the case that
   *     {@code oldCount == newCount}, the method will return {@code true} if
   *     the condition was met.
   * @throws IllegalArgumentException if {@code oldCount} or {@code newCount} is
   *     negative
   */
  @Override public boolean setCount(E element, int oldCount, int newCount) {
    checkNonnegative(oldCount, "oldCount");
    checkNonnegative(newCount, "newCount");
    if (newCount == 0) {
      if (oldCount == 0) {
        // No change to make, but must return true if the element is not present
        return !countMap.containsKey(element);
      } else {
        return countMap.remove(element, oldCount);
      }
    }
    if (oldCount == 0) {
      return countMap.putIfAbsent(element, newCount) == null;
    }
    return countMap.replace(element, oldCount, newCount);
  }

  // Views

  @Override Set<E> createElementSet() {
    final Set<E> delegate = countMap.keySet();
    return new ForwardingSet<E>() {
      @Override protected Set<E> delegate() {
        return delegate;
      }
      @Override public boolean remove(Object object) {
        try {
          return delegate.remove(object);
        } catch (NullPointerException e) {
          return false;
        } catch (ClassCastException e) {
          return false;
        }
      }
    };
  }

  private transient EntrySet entrySet;

  @Override public Set<Multiset.Entry<E>> entrySet() {
    EntrySet result = entrySet;
    if (result == null) {
      entrySet = result = new EntrySet();
    }
    return result;
  }

  private class EntrySet extends AbstractSet<Multiset.Entry<E>> {
    @Override public int size() {
      return countMap.size();
    }

    @Override public boolean isEmpty() {
      return countMap.isEmpty();
    }

    @Override public boolean contains(Object object) {
      if (object instanceof Multiset.Entry) {
        Multiset.Entry<?> entry = (Multiset.Entry<?>) object;
        Object element = entry.getElement();
        int entryCount = entry.getCount();
        return entryCount > 0 && count(element) == entryCount;
      }
      return false;
    }

    @Override public Iterator<Multiset.Entry<E>> iterator() {
      final Iterator<Map.Entry<E, Integer>> backingIterator
          = countMap.entrySet().iterator();
      return new Iterator<Multiset.Entry<E>>() {
        public boolean hasNext() {
          return backingIterator.hasNext();
        }

        public Multiset.Entry<E> next() {
          Map.Entry<E, Integer> backingEntry = backingIterator.next();
          return Multisets.immutableEntry(
              backingEntry.getKey(), backingEntry.getValue());
        }

        public void remove() {
          backingIterator.remove();
        }
      };
    }

    /*
     * Note: the superclass toArray() methods assume that size() gives a correct
     * answer, which ours does not.
     */

    @Override public Object[] toArray() {
      return snapshot().toArray();
    }

    @Override public <T> T[] toArray(T[] array) {
      return snapshot().toArray(array);
    }

    /*
     * We'd love to use 'new ArrayList(this)' or 'list.addAll(this)', but
     * either of these would recurse back to us again!
     */
    private List<Multiset.Entry<E>> snapshot() {
      List<Multiset.Entry<E>> list = Lists.newArrayListWithExpectedSize(size());
      for (Multiset.Entry<E> entry : this) {
        list.add(entry);
      }
      return list;
    }

    @Override public boolean remove(Object object) {
      if (object instanceof Multiset.Entry) {
        Multiset.Entry<?> entry = (Multiset.Entry<?>) object;
        Object element = entry.getElement();
        int entryCount = entry.getCount();
        return countMap.remove(element, entryCount);
      }
      return false;
    }

    @Override public void clear() {
      countMap.clear();
    }

    /**
     * The hash code is the same as countMap's, though the objects aren't equal.
     */
    @Override public int hashCode() {
      return countMap.hashCode();
    }
  }

  /**
   * We use a special form of unboxing that treats null as zero.
   */
  private static int unbox(Integer i) {
    return (i == null) ? 0 : i;
  }

  /**
   * @serialData the number of distinct elements, the first element, its count,
   *     the second element, its count, and so on
   */
  private void writeObject(ObjectOutputStream stream) throws IOException {
    stream.defaultWriteObject();
    // creating HashMultiset to handle concurrent changes
    Serialization.writeMultiset(HashMultiset.create(this), stream);
  }

  private void readObject(ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    FieldSettersHolder.COUNT_MAP_FIELD_SETTER.set(
        this, new ConcurrentHashMap<Object, Object>());
    Serialization.populateMultiset(this, stream);
  }

  private static final long serialVersionUID = 0L;
}
