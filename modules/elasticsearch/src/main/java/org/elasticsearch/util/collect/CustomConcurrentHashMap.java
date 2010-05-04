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

import org.elasticsearch.util.base.Function;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A framework for concurrent hash map implementations. The
 * CustomConcurrentHashMap class itself is not extensible and does not contain
 * any methods. Use {@link Builder} to create a custom concurrent hash map
 * instance. Client libraries implement {@link Strategy}, and this class
 * provides the surrounding concurrent data structure which implements {@link
 * ConcurrentMap}. Additionally supports implementing maps where {@link
 * Map#get} atomically computes values on demand (see {@link
 * Builder#buildComputingMap(CustomConcurrentHashMap.ComputingStrategy,
 * Function)}).
 *
 * <p>The resulting hash table supports full concurrency of retrievals and
 * adjustable expected concurrency for updates. Even though all operations are
 * thread-safe, retrieval operations do <i>not</i> entail locking,
 * and there is <i>not</i> any support for locking the entire table
 * in a way that prevents all access.
 *
 * <p>Retrieval operations (including {@link Map#get}) generally do not
 * block, so may overlap with update operations (including
 * {@link Map#put} and {@link Map#remove}). Retrievals reflect the results
 * of the most recently <i>completed</i> update operations holding
 * upon their onset. For aggregate operations such as {@link Map#putAll}
 * and {@link Map#clear}, concurrent retrievals may reflect insertion or
 * removal of only some entries. Similarly, iterators return elements
 * reflecting the state of the hash table at some point at or since the
 * creation of the iterator. They do <i>not</i> throw
 * {@link java.util.ConcurrentModificationException}. However, iterators can
 * only be used by one thread at a time.
 *
 * <p>The resulting {@link ConcurrentMap} and its views and iterators implement
 * all of the <i>optional</i> methods of the {@link java.util.Map} and {@link
 * java.util.Iterator} interfaces. Partially reclaimed entries are never
 * exposed through the views or iterators.
 *
 * <p>For example, the following strategy emulates the behavior of
 * {@link java.util.concurrent.ConcurrentHashMap}:
 *
 * <pre> {@code
 * class ConcurrentHashMapStrategy<K, V>
 *     implements CustomConcurrentHashMap.Strategy<K, V,
 *     InternalEntry<K, V>>, Serializable {
 *   public InternalEntry<K, V> newEntry(K key, int hash,
 *       InternalEntry<K, V> next) {
 *     return new InternalEntry<K, V>(key, hash, null, next);
 *   }
 *   public InternalEntry<K, V> copyEntry(K key,
 *       InternalEntry<K, V> original, InternalEntry<K, V> next) {
 *     return new InternalEntry<K, V>(key, original.hash, original.value, next);
 *   }
 *   public void setValue(InternalEntry<K, V> entry, V value) {
 *     entry.value = value;
 *   }
 *   public V getValue(InternalEntry<K, V> entry) { return entry.value; }
 *   public boolean equalKeys(K a, Object b) { return a.equals(b); }
 *   public boolean equalValues(V a, Object b) { return a.equals(b); }
 *   public int hashKey(Object key) { return key.hashCode(); }
 *   public K getKey(InternalEntry<K, V> entry) { return entry.key; }
 *   public InternalEntry<K, V> getNext(InternalEntry<K, V> entry) {
 *     return entry.next;
 *   }
 *   public int getHash(InternalEntry<K, V> entry) { return entry.hash; }
 *   public void setInternals(CustomConcurrentHashMap.Internals<K, V,
 *       InternalEntry<K, V>> internals) {} // ignored
 * }
 *
 * class InternalEntry<K, V> {
 *   final K key;
 *   final int hash;
 *   volatile V value;
 *   final InternalEntry<K, V> next;
 *   InternalEntry(K key, int hash, V value, InternalEntry<K, V> next) {
 *     this.key = key;
 *     this.hash = hash;
 *     this.value = value;
 *     this.next = next;
 *   }
 * }
 * }</pre>
 *
 * To create a {@link java.util.concurrent.ConcurrentMap} using the strategy
 * above:
 *
 * <pre>{@code
 *   ConcurrentMap<K, V> map = new CustomConcurrentHashMap.Builder()
 *       .build(new ConcurrentHashMapStrategy<K, V>());
 * }</pre>
 *
 * @author Bob Lee
 * @author Doug Lea
 */
final class CustomConcurrentHashMap {

  /** Prevents instantiation. */
  private CustomConcurrentHashMap() {}

  /**
   * Builds a custom concurrent hash map.
   */
  static final class Builder {
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final int DEFAULT_CONCURRENCY_LEVEL = 16;
    
    private static final int UNSET_INITIAL_CAPACITY = -1;
    private static final int UNSET_CONCURRENCY_LEVEL = -1;
    
    int initialCapacity = UNSET_INITIAL_CAPACITY;
    int concurrencyLevel = UNSET_CONCURRENCY_LEVEL;

    /**
     * Sets a custom initial capacity (defaults to 16). Resizing this or any
     * other kind of hash table is a relatively slow operation, so, when
     * possible, it is a good idea to provide estimates of expected table
     * sizes.
     *
     * @throws IllegalArgumentException if initialCapacity < 0
     */
    public Builder initialCapacity(int initialCapacity) {
      if (this.initialCapacity != UNSET_INITIAL_CAPACITY) {
        throw new IllegalStateException(
            "initial capacity was already set to " + this.initialCapacity);
      }
      if (initialCapacity < 0) {
        throw new IllegalArgumentException();
      }
      this.initialCapacity = initialCapacity;
      return this;
    }

    /**
     * Guides the allowed concurrency among update operations. Used as a
     * hint for internal sizing. The table is internally partitioned to try to
     * permit the indicated number of concurrent updates without contention.
     * Because placement in hash tables is essentially random, the actual
     * concurrency will vary. Ideally, you should choose a value to accommodate
     * as many threads as will ever concurrently modify the table. Using a
     * significantly higher value than you need can waste space and time,
     * and a significantly lower value can lead to thread contention. But
     * overestimates and underestimates within an order of magnitude do
     * not usually have much noticeable impact. A value of one is
     * appropriate when it is known that only one thread will modify and
     * all others will only read. Defaults to {@literal 16}.
     *
     * @throws IllegalArgumentException if concurrencyLevel < 0
     */
    public Builder concurrencyLevel(int concurrencyLevel) {
      if (this.concurrencyLevel != UNSET_CONCURRENCY_LEVEL) {
        throw new IllegalStateException(
            "concurrency level was already set to " + this.concurrencyLevel);
      }
      if (concurrencyLevel <= 0) {
        throw new IllegalArgumentException();
      }
      this.concurrencyLevel = concurrencyLevel;
      return this;
    }

    /**
     * Creates a new concurrent hash map backed by the given strategy.
     *
     * @param strategy used to implement and manipulate the entries
     *
     * @param <K> the type of keys to be stored in the returned map
     * @param <V> the type of values to be stored in the returned map
     * @param <E> the type of internal entry to be stored in the returned map
     *
     * @throws NullPointerException if strategy is null
     */
    public <K, V, E> ConcurrentMap<K, V> buildMap(Strategy<K, V, E> strategy) {
      if (strategy == null) {
        throw new NullPointerException("strategy");
      }
      return new Impl<K, V, E>(strategy, this);
    }

    /**
     * Creates a {@link ConcurrentMap}, backed by the given strategy, that
     * supports atomic, on-demand computation of values. {@link Map#get}
     * returns the value corresponding to the given key, atomically computes
     * it using the computer function passed to this builder, or waits for
     * another thread to compute the value if necessary. Only one value will
     * be computed for each key at a given time.
     *
     * <p>If an entry's value has not finished computing yet, query methods
     * besides {@link java.util.Map#get} return immediately as if an entry
     * doesn't exist. In other words, an entry isn't externally visible until
     * the value's computation completes.
     *
     * <p>{@link Map#get} in the returned map implementation throws:
     * <ul>
     * <li>{@link NullPointerException} if the key is null or the
     *  computer returns null</li>
     * <li>or {@link ComputationException} wrapping an exception thrown by the
     *  computation</li>
     * </ul>
     *
     * <p><b>Note:</b> Callers of {@code get()} <i>must</i> ensure that the key
     *  argument is of type {@code K}. {@code Map.get()} takes {@code Object},
     *  so the key type is not checked at compile time. Passing an object of
     *  a type other than {@code K} can result in that object being unsafely
     *  passed to the computer function as type {@code K} not to mention the
     *  unsafe key being stored in the map.
     *
     * @param strategy used to implement and manipulate the entries
     * @param computer used to compute values for keys
     *
     * @param <K> the type of keys to be stored in the returned map
     * @param <V> the type of values to be stored in the returned map
     * @param <E> the type of internal entry to be stored in the returned map
     *
     * @throws NullPointerException if strategy or computer is null
     */
    public <K, V, E> ConcurrentMap<K, V> buildComputingMap(
        ComputingStrategy<K, V, E> strategy,
        Function<? super K, ? extends V> computer) {
      if (strategy == null) {
        throw new NullPointerException("strategy");
      }
      if (computer == null) {
        throw new NullPointerException("computer");
      }

      return new ComputingImpl<K, V, E>(strategy, this, computer);
    }
    
    int getInitialCapacity() {
      return (initialCapacity == UNSET_INITIAL_CAPACITY)
          ? DEFAULT_INITIAL_CAPACITY : initialCapacity;
    }

    int getConcurrencyLevel() {
      return (concurrencyLevel == UNSET_CONCURRENCY_LEVEL)
          ? DEFAULT_CONCURRENCY_LEVEL : concurrencyLevel;
    }
  }

  /**
   * Implements behavior specific to the client's concurrent hash map
   * implementation. Used by the framework to create new entries and perform
   * operations on them.
   *
   * <p>Method parameters are never null unless otherwise specified.
   *
   * <h3>Partially Reclaimed Entries</h3>
   *
   * <p>This class does <i>not</i> allow {@code null} to be used as a key.
   * Setting values to null is not permitted, but entries may have null keys
   * or values for various reasons. For example, the key or value may have
   * been garbage collected or reclaimed through other means.
   * CustomConcurrentHashMap treats entries with null keys and values as
   * "partially reclaimed" and ignores them for the most part. Entries may
   * enter a partially reclaimed state but they must not leave it. Partially
   * reclaimed entries will not be copied over during table expansions, for
   * example. Strategy implementations should proactively remove partially
   * reclaimed entries so that {@link Map#isEmpty} and {@link Map#size()}
   * return up-to-date results.
   *
   * @param <K> the type of keys to be stored in the returned map
   * @param <V> the type of values to be stored in the returned map
   * @param <E> internal entry type, not directly exposed to clients in map
   *  views
   */
  public interface Strategy<K, V, E> {

    /**
     * Constructs a new entry for the given key with a pointer to the given
     * next entry.
     *
     * <p>This method may return different entry implementations
     * depending upon whether next is null or not. For example, if next is
     * null (as will often be the case), this factory might use an entry
     * class that doesn't waste memory on an unnecessary field.
     *
     * @param key for this entry
     * @param hash of key returned by {@link #hashKey}
     * @param next entry (used when implementing a hash bucket as a linked
     *  list, for example), possibly null
     * @return a new entry
     */
    abstract E newEntry(K key, int hash, E next);

    /**
     * Creates a copy of the given entry pointing to the given next entry.
     * Copies the value and any other implementation-specific state from
     * {@code original} to the returned entry. For example,
     * CustomConcurrentHashMap might use this method when removing other
     * entries or expanding the internal table.
     *
     * @param key for {@code original} as well as the returned entry.
     *  Explicitly provided here to prevent reclamation of the key at an
     *  inopportune time in implementations that don't otherwise keep
     *  a strong reference to the key.
     * @param original entry from which the value and other
     *  implementation-specific state should be copied
     * @param newNext the next entry the new entry should point to, possibly
     *  null
     */
    E copyEntry(K key, E original, E newNext);

    /**
     * Sets the value of an entry using volatile semantics. Values are set
     * synchronously on a per-entry basis.
     *
     * @param entry to set the value on
     * @param value to set
     */
    void setValue(E entry, V value);

    /**
     * Gets the value of an entry using volatile semantics.
     *
     * @param entry to get the value from
     */
    V getValue(E entry);

    /**
     * Returns true if the two given keys are equal, false otherwise. Neither
     * key will be null.
     *
     * @param a key from inside the map
     * @param b key passed from caller, not necesarily of type K
     *
     * @see Object#equals the same contractual obligations apply here
     */
    boolean equalKeys(K a, Object b);

    /**
     * Returns true if the two given values are equal, false otherwise. Neither
     * value will be null.
     *
     * @param a value from inside the map
     * @param b value passed from caller, not necesarily of type V
     *
     * @see Object#equals the same contractual obligations apply here
     */
    boolean equalValues(V a, Object b);

    /**
     * Returns a hash code for the given key.
     *
     * @see Object#hashCode the same contractual obligations apply here
     */
    int hashKey(Object key);

    /**
     * Gets the key for the given entry. This may return null (for example,
     * if the key was reclaimed by the garbage collector).
     *
     * @param entry to get key from
     * @return key from the given entry
     */
    K getKey(E entry);

    /**
     * Gets the next entry relative to the given entry, the exact same entry
     * that was provided to {@link Strategy#newEntry} when the given entry was
     * created.
     *
     * @return the next entry or null if null was passed to
     *  {@link Strategy#newEntry}
     */
    E getNext(E entry);

    /**
     * Returns the hash code that was passed to {@link Strategy#newEntry})
     * when the given entry was created.
     */
    int getHash(E entry);

// TODO:
//    /**
//     * Notifies the strategy that an entry has been removed from the map.
//     *
//     * @param entry that was recently removed
//     */
//    void remove(E entry);

    /**
     * Provides an API for interacting directly with the map's internal
     * entries to this strategy. Invoked once when the map is created.
     * Strategies that don't need access to the map's internal entries
     * can simply ignore the argument.
     *
     * @param internals of the map, enables direct interaction with the
     *  internal entries
     */
    void setInternals(Internals<K, V, E> internals);
  }

  /**
   * Provides access to a map's internal entries.
   */
  public interface Internals<K, V, E> {

// TODO:
//    /**
//     * Returns a set view of the internal entries.
//     */
//    Set<E> entrySet();

    /**
     * Returns the internal entry corresponding to the given key from the map.
     *
     * @param key to retrieve entry for
     *
     * @throws NullPointerException if key is null
     */
    E getEntry(K key);

    /**
     * Removes the given entry from the map if the value of the entry in the
     * map matches the given value.
     *
     * @param entry to remove
     * @param value entry must have for the removal to succeed
     *
     * @throws NullPointerException if entry is null
     */
    boolean removeEntry(E entry, @Nullable V value);

    /**
     * Removes the given entry from the map.
     *
     * @param entry to remove
     *
     * @throws NullPointerException if entry is null
     */
    boolean removeEntry(E entry);
  }

  /**
   * Extends {@link Strategy} to add support for computing values on-demand.
   * Implementations should typically intialize the entry's value to a
   * placeholder value in {@link #newEntry(Object, int, Object)} so that
   * {@link #waitForValue(Object)} can tell the difference between a
   * pre-intialized value and an in-progress computation. {@link
   * #copyEntry(Object, Object, Object)} must detect and handle the case where
   * an entry is copied in the middle of a computation. Implementations can
   * throw {@link UnsupportedOperationException} in {@link #setValue(Object,
   * Object)} if they wish to prevent users from setting values directly.
   *
   * @see Builder#buildComputingMap(CustomConcurrentHashMap.ComputingStrategy,
   *     Function)
   */
  public interface ComputingStrategy<K, V, E> extends Strategy<K, V, E> {

    /**
     * Computes a value for the given key and stores it in the given entry.
     * Called as a result of {@link Map#get}. If this method throws an
     * exception, CustomConcurrentHashMap will remove the entry and retry
     * the computation on subsequent requests.
     *
     * @param entry that was created
     * @param computer passed to {@link Builder#buildMap}
     *
     * @throws ComputationException if the computation threw an exception
     * @throws NullPointerException if the computer returned null
     *
     * @return the computed value
     */
    V compute(K key, E entry, Function<? super K, ? extends V> computer);

    /**
     * Gets a value from an entry waiting for the value to be set by {@link
     * #compute} if necessary. Returns null if a value isn't available at
     * which point CustomConcurrentHashMap tries to compute a new value.
     *
     * @param entry to return value from
     * @return stored value or null if the value isn't available
     *
     * @throws InterruptedException if the thread was interrupted while
     *  waiting
     */
    V waitForValue(E entry) throws InterruptedException;
  }

  /**
   * Applies a supplemental hash function to a given hash code, which defends
   * against poor quality hash functions. This is critical when the
   * concurrent hash map uses power-of-two length hash tables, that otherwise
   * encounter collisions for hash codes that do not differ in lower or upper
   * bits.
   *
   * @param h hash code
   */
  private static int rehash(int h) {
    // Spread bits to regularize both segment and index locations,
    // using variant of single-word Wang/Jenkins hash.
    h += (h << 15) ^ 0xffffcd7d;
    h ^= (h >>> 10);
    h += (h << 3);
    h ^= (h >>> 6);
    h += (h << 2) + (h << 14);
    return h ^ (h >>> 16);
  }

  /** The concurrent hash map implementation. */
  static class Impl<K, V, E> extends AbstractMap<K, V>
      implements ConcurrentMap<K, V>, Serializable {

    /*
     * The basic strategy is to subdivide the table among Segments,
     * each of which itself is a concurrently readable hash table.
     */

    /* ---------------- Constants -------------- */

    /**
     * The maximum capacity, used if a higher value is implicitly specified by
     * either of the constructors with arguments.  MUST be a power of two <=
     * 1<<30 to ensure that entries are indexable using ints.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The maximum number of segments to allow; used to bound constructor
     * arguments.
     */
    static final int MAX_SEGMENTS = 1 << 16; // slightly conservative

    /**
     * Number of unsynchronized retries in size and containsValue methods before
     * resorting to locking. This is used to avoid unbounded retries if tables
     * undergo continuous modification which would make it impossible to obtain
     * an accurate result.
     */
    static final int RETRIES_BEFORE_LOCK = 2;

    /* ---------------- Fields -------------- */

    /**
     * The strategy used to implement this map.
     */
    final Strategy<K, V, E> strategy;

    /**
     * Mask value for indexing into segments. The upper bits of a key's hash
     * code are used to choose the segment.
     */
    final int segmentMask;

    /**
     * Shift value for indexing within segments. Helps prevent entries that
     * end up in the same segment from also ending up in the same bucket.
     */
    final int segmentShift;

    /**
     * The segments, each of which is a specialized hash table
     */
    final Segment[] segments;

    /**
     * Creates a new, empty map with the specified strategy, initial capacity,
     * load factor and concurrency level.
     */
    Impl(Strategy<K, V, E> strategy, Builder builder) {
      int concurrencyLevel = builder.getConcurrencyLevel();
      int initialCapacity = builder.getInitialCapacity();

      if (concurrencyLevel > MAX_SEGMENTS) {
        concurrencyLevel = MAX_SEGMENTS;
      }

      // Find power-of-two sizes best matching arguments
      int segmentShift = 0;
      int segmentCount = 1;
      while (segmentCount < concurrencyLevel) {
        ++segmentShift;
        segmentCount <<= 1;
      }
      this.segmentShift = 32 - segmentShift;
      segmentMask = segmentCount - 1;
      this.segments = newSegmentArray(segmentCount);

      if (initialCapacity > MAXIMUM_CAPACITY) {
        initialCapacity = MAXIMUM_CAPACITY;
      }

      int segmentCapacity = initialCapacity / segmentCount;
      if (segmentCapacity * segmentCount < initialCapacity) {
        ++segmentCapacity;
      }

      int segmentSize = 1;
      while (segmentSize < segmentCapacity) {
          segmentSize <<= 1;
      }
      for (int i = 0; i < this.segments.length; ++i) {
        this.segments[i] = new Segment(segmentSize);
      }

      this.strategy = strategy;

      strategy.setInternals(new InternalsImpl());
    }

    int hash(Object key) {
      int h = strategy.hashKey(key);
      return rehash(h);
    }

    class InternalsImpl implements Internals<K, V, E>, Serializable {

      static final long serialVersionUID = 0;

      public E getEntry(K key) {
        if (key == null) {
          throw new NullPointerException("key");
        }
        int hash = hash(key);
        return segmentFor(hash).getEntry(key, hash);
      }

      public boolean removeEntry(E entry, V value) {
        if (entry == null) {
          throw new NullPointerException("entry");
        }
        int hash = strategy.getHash(entry);
        return segmentFor(hash).removeEntry(entry, hash, value);
      }

      public boolean removeEntry(E entry) {
        if (entry == null) {
          throw new NullPointerException("entry");
        }
        int hash = strategy.getHash(entry);
        return segmentFor(hash).removeEntry(entry, hash);
      }
    }

    @SuppressWarnings("unchecked")
    Segment[] newSegmentArray(int ssize) {
      // Note: This is the only way I could figure out how to create
      // a segment array (the compile has a tough time with arrays of
      // inner classes of generic types apparently). Safe because we're
      // restricting what can go in the array and no one has an
      // unrestricted reference.
      return (Segment[]) Array.newInstance(Segment.class, ssize);
    }

    /* ---------------- Small Utilities -------------- */

    /**
     * Returns the segment that should be used for key with given hash
     *
     * @param hash the hash code for the key
     * @return the segment
     */
    Segment segmentFor(int hash) {
      return segments[(hash >>> segmentShift) & segmentMask];
    }

    /* ---------------- Inner Classes -------------- */

    /**
     * Segments are specialized versions of hash tables.  This subclasses from
     * ReentrantLock opportunistically, just to simplify some locking and avoid
     * separate construction.
     */
    @SuppressWarnings("serial") // This class is never serialized.
    final class Segment extends ReentrantLock {

      /*
       * Segments maintain a table of entry lists that are ALWAYS
       * kept in a consistent state, so can be read without locking.
       * Next fields of nodes are immutable (final).  All list
       * additions are performed at the front of each bin. This
       * makes it easy to check changes, and also fast to traverse.
       * When nodes would otherwise be changed, new nodes are
       * created to replace them. This works well for hash tables
       * since the bin lists tend to be short. (The average length
       * is less than two for the default load factor threshold.)
       *
       * Read operations can thus proceed without locking, but rely
       * on selected uses of volatiles to ensure that completed
       * write operations performed by other threads are
       * noticed. For most purposes, the "count" field, tracking the
       * number of elements, serves as that volatile variable
       * ensuring visibility.  This is convenient because this field
       * needs to be read in many read operations anyway:
       *
       *   - All (unsynchronized) read operations must first read the
       *     "count" field, and should not look at table entries if
       *     it is 0.
       *
       *   - All (synchronized) write operations should write to
       *     the "count" field after structurally changing any bin.
       *     The operations must not take any action that could even
       *     momentarily cause a concurrent read operation to see
       *     inconsistent data. This is made easier by the nature of
       *     the read operations in Map. For example, no operation
       *     can reveal that the table has grown but the threshold
       *     has not yet been updated, so there are no atomicity
       *     requirements for this with respect to reads.
       *
       * As a guide, all critical volatile reads and writes to the
       * count field are marked in code comments.
       */

      /**
       * The number of elements in this segment's region.
       */
      volatile int count;

      /**
       * Number of updates that alter the size of the table. This is used
       * during bulk-read methods to make sure they see a consistent snapshot:
       * If modCounts change during a traversal of segments computing size or
       * checking containsValue, then we might have an inconsistent view of
       * state so (usually) must retry.
       */
      int modCount;

      /**
       * The table is expanded when its size exceeds this threshold. (The
       * value of this field is always {@code (int)(capacity * loadFactor)}.)
       */
      int threshold;

      /**
       * The per-segment table.
       */
      volatile AtomicReferenceArray<E> table;

      Segment(int initialCapacity) {
        setTable(newEntryArray(initialCapacity));
      }

      AtomicReferenceArray<E> newEntryArray(int size) {
        return new AtomicReferenceArray<E>(size);
      }

      /**
       * Sets table to new HashEntry array. Call only while holding lock or in
       * constructor.
       */
      void setTable(AtomicReferenceArray<E> newTable) {
        this.threshold = newTable.length() * 3 / 4;
        this.table = newTable;
      }

      /**
       * Returns properly casted first entry of bin for given hash.
       */
      E getFirst(int hash) {
        AtomicReferenceArray<E> table = this.table;
        return table.get(hash & (table.length() - 1));
      }

      /* Specialized implementations of map methods */

      public E getEntry(Object key, int hash) {
        Strategy<K, V, E> s = Impl.this.strategy;
        if (count != 0) { // read-volatile
          for (E e = getFirst(hash); e != null; e = s.getNext(e)) {
            if (s.getHash(e) != hash) {
              continue;
            }

            K entryKey = s.getKey(e);
            if (entryKey == null) {
              continue;
            }

            if (s.equalKeys(entryKey, key)) {
              return e;
            }
          }
        }

        return null;
      }

      V get(Object key, int hash) {
        E entry = getEntry(key, hash);
        if (entry == null) {
          return null;
        }

        return strategy.getValue(entry);
      }

      boolean containsKey(Object key, int hash) {
        Strategy<K, V, E> s = Impl.this.strategy;
        if (count != 0) { // read-volatile
          for (E e = getFirst(hash); e != null; e = s.getNext(e)) {
            if (s.getHash(e) != hash) {
              continue;
            }

            K entryKey = s.getKey(e);
            if (entryKey == null) {
              continue;
            }

            if (s.equalKeys(entryKey, key)) {
              // Return true only if this entry has a value.
              return s.getValue(e) != null;
            }
          }
        }

        return false;
      }

      boolean containsValue(Object value) {
        Strategy<K, V, E> s = Impl.this.strategy;
        if (count != 0) { // read-volatile
          AtomicReferenceArray<E> table = this.table;
          int length = table.length();
          for (int i = 0; i < length; i++) {
            for (E e = table.get(i); e != null; e = s.getNext(e)) {
              V entryValue = s.getValue(e);

              // If the value disappeared, this entry is partially collected,
              // and we should skip it.
              if (entryValue == null) {
                continue;
              }

              if (s.equalValues(entryValue, value)) {
                return true;
              }
            }
          }
        }

        return false;
      }

      boolean replace(K key, int hash, V oldValue, V newValue) {
        Strategy<K, V, E> s = Impl.this.strategy;
        lock();
        try {
          for (E e = getFirst(hash); e != null; e = s.getNext(e)) {
            K entryKey = s.getKey(e);
            if (s.getHash(e) == hash && entryKey != null
                && s.equalKeys(key, entryKey)) {
              // If the value disappeared, this entry is partially collected,
              // and we should pretend like it doesn't exist.
              V entryValue = s.getValue(e);
              if (entryValue == null) {
                return false;
              }

              if (s.equalValues(entryValue, oldValue)) {
                s.setValue(e, newValue);
                return true;
              }
            }
          }

          return false;
        } finally {
          unlock();
        }
      }

      V replace(K key, int hash, V newValue) {
        Strategy<K, V, E> s = Impl.this.strategy;
        lock();
        try {
          for (E e = getFirst(hash); e != null; e = s.getNext(e)) {
            K entryKey = s.getKey(e);
            if (s.getHash(e) == hash && entryKey != null
                && s.equalKeys(key, entryKey)) {
              // If the value disappeared, this entry is partially collected,
              // and we should pretend like it doesn't exist.
              V entryValue = s.getValue(e);
              if (entryValue == null) {
                return null;
              }

              s.setValue(e, newValue);
              return entryValue;
            }
          }

          return null;
        } finally {
          unlock();
        }
      }

      V put(K key, int hash, V value, boolean onlyIfAbsent) {
        Strategy<K, V, E> s = Impl.this.strategy;
        lock();
        try {
          int count = this.count;
          if (count++ > this.threshold) { // ensure capacity
            expand();
          }

          AtomicReferenceArray<E> table = this.table;
          int index = hash & (table.length() - 1);

          E first = table.get(index);

          // Look for an existing entry.
          for (E e = first; e != null; e = s.getNext(e)) {
            K entryKey = s.getKey(e);
            if (s.getHash(e) == hash && entryKey != null
                && s.equalKeys(key, entryKey)) {
              // We found an existing entry.

              // If the value disappeared, this entry is partially collected,
              // and we should pretend like it doesn't exist.
              V entryValue = s.getValue(e);
              if (onlyIfAbsent && entryValue != null) {
                return entryValue;
              }

              s.setValue(e, value);
              return entryValue;
            }
          }

          // Create a new entry.
          ++modCount;
          E newEntry = s.newEntry(key, hash, first);
          s.setValue(newEntry, value);
          table.set(index, newEntry);
          this.count = count; // write-volatile
          return null;
        } finally {
          unlock();
        }
      }

      /**
       * Expands the table if possible.
       */
      void expand() {
        AtomicReferenceArray<E> oldTable = table;
        int oldCapacity = oldTable.length();
        if (oldCapacity >= MAXIMUM_CAPACITY) {
          return;
        }

        /*
         * Reclassify nodes in each list to new Map.  Because we are
         * using power-of-two expansion, the elements from each bin
         * must either stay at same index, or move with a power of two
         * offset. We eliminate unnecessary node creation by catching
         * cases where old nodes can be reused because their next
         * fields won't change. Statistically, at the default
         * threshold, only about one-sixth of them need cloning when
         * a table doubles. The nodes they replace will be garbage
         * collectable as soon as they are no longer referenced by any
         * reader thread that may be in the midst of traversing table
         * right now.
         */

        Strategy<K, V, E> s = Impl.this.strategy;
        AtomicReferenceArray<E> newTable = newEntryArray(oldCapacity << 1);
        threshold = newTable.length() * 3 / 4;
        int newMask = newTable.length() - 1;
        for (int oldIndex = 0; oldIndex < oldCapacity; oldIndex++) {
          // We need to guarantee that any existing reads of old Map can
          //  proceed. So we cannot yet null out each bin.
          E head = oldTable.get(oldIndex);

          if (head != null) {
            E next = s.getNext(head);
            int headIndex = s.getHash(head) & newMask;

            // Single node on list
            if (next == null) {
              newTable.set(headIndex, head);
            } else {
              // Reuse the consecutive sequence of nodes with the same target
              // index from the end of the list. tail points to the first
              // entry in the reusable list.
              E tail = head;
              int tailIndex = headIndex;
              for (E last = next; last != null; last = s.getNext(last)) {
                int newIndex = s.getHash(last) & newMask;
                if (newIndex != tailIndex) {
                  // The index changed. We'll need to copy the previous entry.
                  tailIndex = newIndex;
                  tail = last;
                }
              }
              newTable.set(tailIndex, tail);

              // Clone nodes leading up to the tail.
              for (E e = head; e != tail; e = s.getNext(e)) {
                K key = s.getKey(e);
                if (key != null) {
                  int newIndex = s.getHash(e) & newMask;
                  E newNext = newTable.get(newIndex);
                  newTable.set(newIndex, s.copyEntry(key, e, newNext));
                } else {
                  // Key was reclaimed. Skip entry.
                }
              }
            }
          }
        }
        table = newTable;
      }

      V remove(Object key, int hash) {
        Strategy<K, V, E> s = Impl.this.strategy;
        lock();
        try {
          int count = this.count - 1;
          AtomicReferenceArray<E> table = this.table;
          int index = hash & (table.length() - 1);
          E first = table.get(index);

          for (E e = first; e != null; e = s.getNext(e)) {
            K entryKey = s.getKey(e);
            if (s.getHash(e) == hash && entryKey != null
                && s.equalKeys(entryKey, key)) {
              V entryValue = strategy.getValue(e);
              // All entries following removed node can stay
              // in list, but all preceding ones need to be
              // cloned.
              ++modCount;
              E newFirst = s.getNext(e);
              for (E p = first; p != e; p = s.getNext(p)) {
                K pKey = s.getKey(p);
                if (pKey != null) {
                  newFirst = s.copyEntry(pKey, p, newFirst);
                } else {
                  // Key was reclaimed. Skip entry.
                }
              }
              table.set(index, newFirst);
              this.count = count; // write-volatile
              return entryValue;
            }
          }

          return null;
        } finally {
          unlock();
        }
      }

      boolean remove(Object key, int hash, Object value) {
        Strategy<K, V, E> s = Impl.this.strategy;
        lock();
        try {
          int count = this.count - 1;
          AtomicReferenceArray<E> table = this.table;
          int index = hash & (table.length() - 1);
          E first = table.get(index);

          for (E e = first; e != null; e = s.getNext(e)) {
            K entryKey = s.getKey(e);
            if (s.getHash(e) == hash && entryKey != null
                && s.equalKeys(entryKey, key)) {
              V entryValue = strategy.getValue(e);
              if (value == entryValue || (value != null && entryValue != null
                  && s.equalValues(entryValue, value))) {
                // All entries following removed node can stay
                // in list, but all preceding ones need to be
                // cloned.
                ++modCount;
                E newFirst = s.getNext(e);
                for (E p = first; p != e; p = s.getNext(p)) {
                  K pKey = s.getKey(p);
                  if (pKey != null) {
                    newFirst = s.copyEntry(pKey, p, newFirst);
                  } else {
                    // Key was reclaimed. Skip entry.
                  }
                }
                table.set(index, newFirst);
                this.count = count; // write-volatile
                return true;
              } else {
                return false;
              }
            }
          }

          return false;
        } finally {
          unlock();
        }
      }

      public boolean removeEntry(E entry, int hash, V value) {
        Strategy<K, V, E> s = Impl.this.strategy;
        lock();
        try {
          int count = this.count - 1;
          AtomicReferenceArray<E> table = this.table;
          int index = hash & (table.length() - 1);
          E first = table.get(index);

          for (E e = first; e != null; e = s.getNext(e)) {
            if (s.getHash(e) == hash && entry.equals(e)) {
              V entryValue = s.getValue(e);
              if (entryValue == value || (value != null
                  && s.equalValues(entryValue, value))) {
                // All entries following removed node can stay
                // in list, but all preceding ones need to be
                // cloned.
                ++modCount;
                E newFirst = s.getNext(e);
                for (E p = first; p != e; p = s.getNext(p)) {
                  K pKey = s.getKey(p);
                  if (pKey != null) {
                    newFirst = s.copyEntry(pKey, p, newFirst);
                  } else {
                    // Key was reclaimed. Skip entry.
                  }
                }
                table.set(index, newFirst);
                this.count = count; // write-volatile
                return true;
              } else {
                return false;
              }
            }
          }

          return false;
        } finally {
          unlock();
        }
      }

      public boolean removeEntry(E entry, int hash) {
        Strategy<K, V, E> s = Impl.this.strategy;
        lock();
        try {
          int count = this.count - 1;
          AtomicReferenceArray<E> table = this.table;
          int index = hash & (table.length() - 1);
          E first = table.get(index);

          for (E e = first; e != null; e = s.getNext(e)) {
            if (s.getHash(e) == hash && entry.equals(e)) {
              // All entries following removed node can stay
              // in list, but all preceding ones need to be
              // cloned.
              ++modCount;
              E newFirst = s.getNext(e);
              for (E p = first; p != e; p = s.getNext(p)) {
                K pKey = s.getKey(p);
                if (pKey != null) {
                  newFirst = s.copyEntry(pKey, p, newFirst);
                } else {
                  // Key was reclaimed. Skip entry.
                }
              }
              table.set(index, newFirst);
              this.count = count; // write-volatile
              return true;
            }
          }

          return false;
        } finally {
          unlock();
        }
      }

      void clear() {
        if (count != 0) {
          lock();
          try {
            AtomicReferenceArray<E> table = this.table;
            for (int i = 0; i < table.length(); i++) {
              table.set(i, null);
            }
            ++modCount;
            count = 0; // write-volatile
          } finally {
            unlock();
          }
        }
      }
    }

    /* ---------------- Public operations -------------- */

    /**
     * Returns {@code true} if this map contains no key-value mappings.
     *
     * @return {@code true} if this map contains no key-value mappings
     */
    @Override public boolean isEmpty() {
      final Segment[] segments = this.segments;
      /*
       * We keep track of per-segment modCounts to avoid ABA
       * problems in which an element in one segment was added and
       * in another removed during traversal, in which case the
       * table was never actually empty at any point. Note the
       * similar use of modCounts in the size() and containsValue()
       * methods, which are the only other methods also susceptible
       * to ABA problems.
       */
      int[] mc = new int[segments.length];
      int mcsum = 0;
      for (int i = 0; i < segments.length; ++i) {
        if (segments[i].count != 0) {
          return false;
        } else {
          mcsum += mc[i] = segments[i].modCount;
        }
      }
      // If mcsum happens to be zero, then we know we got a snapshot
      // before any modifications at all were made.  This is
      // probably common enough to bother tracking.
      if (mcsum != 0) {
        for (int i = 0; i < segments.length; ++i) {
          if (segments[i].count != 0 ||
              mc[i] != segments[i].modCount) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Returns the number of key-value mappings in this map.  If the map
     * contains more than {@code Integer.MAX_VALUE} elements, returns
     * {@code Integer.MAX_VALUE}.
     *
     * @return the number of key-value mappings in this map
     */
    @Override public int size() {
      final Segment[] segments = this.segments;
      long sum = 0;
      long check = 0;
      int[] mc = new int[segments.length];
      // Try a few times to get accurate count. On failure due to
      // continuous async changes in table, resort to locking.
      for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
        check = 0;
        sum = 0;
        int mcsum = 0;
        for (int i = 0; i < segments.length; ++i) {
          sum += segments[i].count;
          mcsum += mc[i] = segments[i].modCount;
        }
        if (mcsum != 0) {
          for (int i = 0; i < segments.length; ++i) {
            check += segments[i].count;
            if (mc[i] != segments[i].modCount) {
              check = -1; // force retry
              break;
            }
          }
        }
        if (check == sum) {
          break;
        }
      }
      if (check != sum) { // Resort to locking all segments
        sum = 0;
        for (Segment segment : segments) {
          segment.lock();
        }
        for (Segment segment : segments) {
          sum += segment.count;
        }
        for (Segment segment : segments) {
          segment.unlock();
        }
      }
      if (sum > Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      } else {
        return (int) sum;
      }
    }

    /**
     * Returns the value to which the specified key is mapped, or {@code null}
     * if this map contains no mapping for the key.
     *
     * <p>More formally, if this map contains a mapping from a key {@code k} to
     * a value {@code v} such that {@code key.equals(k)}, then this method
     * returns {@code v}; otherwise it returns {@code null}.  (There can be at
     * most one such mapping.)
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override public V get(Object key) {
      if (key == null) {
        throw new NullPointerException("key");
      }
      int hash = hash(key);
      return segmentFor(hash).get(key, hash);
    }

    /**
     * Tests if the specified object is a key in this table.
     *
     * @param key possible key
     * @return {@code true} if and only if the specified object is a key in
     *         this table, as determined by the {@code equals} method;
     *         {@code false} otherwise.
     * @throws NullPointerException if the specified key is null
     */
    @Override public boolean containsKey(Object key) {
      if (key == null) {
        throw new NullPointerException("key");
      }
      int hash = hash(key);
      return segmentFor(hash).containsKey(key, hash);
    }

    /**
     * Returns {@code true} if this map maps one or more keys to the specified
     * value. Note: This method requires a full internal traversal of the hash
     * table, and so is much slower than method {@code containsKey}.
     *
     * @param value value whose presence in this map is to be tested
     * @return {@code true} if this map maps one or more keys to the specified
     *         value
     * @throws NullPointerException if the specified value is null
     */
    @Override public boolean containsValue(Object value) {
      if (value == null) {
        throw new NullPointerException("value");
      }

      // See explanation of modCount use above

      final Segment[] segments = this.segments;
      int[] mc = new int[segments.length];

      // Try a few times without locking
      for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
        int mcsum = 0;
        for (int i = 0; i < segments.length; ++i) {
          @SuppressWarnings("UnusedDeclaration")
          int c = segments[i].count;
          mcsum += mc[i] = segments[i].modCount;
          if (segments[i].containsValue(value)) {
            return true;
          }
        }
        boolean cleanSweep = true;
        if (mcsum != 0) {
          for (int i = 0; i < segments.length; ++i) {
            @SuppressWarnings("UnusedDeclaration")
            int c = segments[i].count;
            if (mc[i] != segments[i].modCount) {
              cleanSweep = false;
              break;
            }
          }
        }
        if (cleanSweep) {
          return false;
        }
      }
      // Resort to locking all segments
      for (Segment segment : segments) {
        segment.lock();
      }
      boolean found = false;
      try {
        for (Segment segment : segments) {
          if (segment.containsValue(value)) {
            found = true;
            break;
          }
        }
      } finally {
        for (Segment segment : segments) {
          segment.unlock();
        }
      }
      return found;
    }

    /**
     * Maps the specified key to the specified value in this table. Neither the
     * key nor the value can be null.
     *
     * <p> The value can be retrieved by calling the {@code get} method with a
     * key that is equal to the original key.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with {@code key}, or {@code null}
     *         if there was no mapping for {@code key}
     * @throws NullPointerException if the specified key or value is null
     */
    @Override public V put(K key, V value) {
      if (key == null) {
        throw new NullPointerException("key");
      }
      if (value == null) {
        throw new NullPointerException("value");
      }
      int hash = hash(key);
      return segmentFor(hash).put(key, hash, value, false);
    }

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key, or
     *         {@code null} if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V putIfAbsent(K key, V value) {
      if (key == null) {
        throw new NullPointerException("key");
      }
      if (value == null) {
        throw new NullPointerException("value");
      }
      int hash = hash(key);
      return segmentFor(hash).put(key, hash, value, true);
    }

    /**
     * Copies all of the mappings from the specified map to this one. These
     * mappings replace any mappings that this map had for any of the keys
     * currently in the specified map.
     *
     * @param m mappings to be stored in this map
     */
    @Override public void putAll(Map<? extends K, ? extends V> m) {
      for (Entry<? extends K, ? extends V> e : m.entrySet()) {
        put(e.getKey(), e.getValue());
      }
    }

    /**
     * Removes the key (and its corresponding value) from this map. This method
     * does nothing if the key is not in the map.
     *
     * @param key the key that needs to be removed
     * @return the previous value associated with {@code key}, or {@code null}
     *         if there was no mapping for {@code key}
     * @throws NullPointerException if the specified key is null
     */
    @Override public V remove(Object key) {
      if (key == null) {
        throw new NullPointerException("key");
      }
      int hash = hash(key);
      return segmentFor(hash).remove(key, hash);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified key is null
     */
    public boolean remove(Object key, Object value) {
      if (key == null) {
        throw new NullPointerException("key");
      }
      int hash = hash(key);
      return segmentFor(hash).remove(key, hash, value);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if any of the arguments are null
     */
    public boolean replace(K key, V oldValue, V newValue) {
      if (key == null) {
        throw new NullPointerException("key");
      }
      if (oldValue == null) {
        throw new NullPointerException("oldValue");
      }
      if (newValue == null) {
        throw new NullPointerException("newValue");
      }
      int hash = hash(key);
      return segmentFor(hash).replace(key, hash, oldValue, newValue);
    }

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key, or
     *         {@code null} if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V replace(K key, V value) {
      if (key == null) {
        throw new NullPointerException("key");
      }
      if (value == null) {
        throw new NullPointerException("value");
      }
      int hash = hash(key);
      return segmentFor(hash).replace(key, hash, value);
    }

    /**
     * Removes all of the mappings from this map.
     */
    @Override public void clear() {
      for (Segment segment : segments) {
        segment.clear();
      }
    }

    Set<K> keySet;

    /**
     * Returns a {@link java.util.Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are reflected in the
     * set, and vice-versa. The set supports element removal, which removes the
     * corresponding mapping from this map, via the {@code Iterator.remove},
     * {@code Set.remove}, {@code removeAll}, {@code retainAll}, and
     * {@code clear} operations. It does not support the {@code add} or
     * {@code addAll} operations.
     *
     * <p>The view's {@code iterator} is a "weakly consistent" iterator that
     * will never throw {@link java.util.ConcurrentModificationException}, and
     * guarantees to traverse elements as they existed upon construction of the
     * iterator, and may (but is not guaranteed to) reflect any modifications
     * subsequent to construction.
     */
    @Override public Set<K> keySet() {
      Set<K> ks = keySet;
      return (ks != null) ? ks : (keySet = new KeySet());
    }

    Collection<V> values;

    /**
     * Returns a {@link java.util.Collection} view of the values contained in
     * this map. The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa. The collection supports
     * element removal, which removes the corresponding mapping from this map,
     * via the {@code Iterator.remove}, {@code Collection.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear} operations. It
     * does not support the {@code add} or {@code addAll} operations.
     *
     * <p>The view's {@code iterator} is a "weakly consistent" iterator that
     * will never throw {@link java.util.ConcurrentModificationException}, and
     * guarantees to traverse elements as they existed upon construction of the
     * iterator, and may (but is not guaranteed to) reflect any modifications
     * subsequent to construction.
     */
    @Override public Collection<V> values() {
      Collection<V> vs = values;
      return (vs != null) ? vs : (values = new Values());
    }

    Set<Entry<K, V>> entrySet;

    /**
     * Returns a {@link java.util.Set} view of the mappings contained in this
     * map. The set is backed by the map, so changes to the map are reflected in
     * the set, and vice-versa. The set supports element removal, which removes
     * the corresponding mapping from the map, via the {@code Iterator.remove},
     * {@code Set.remove}, {@code removeAll}, {@code retainAll}, and
     * {@code clear} operations. It does not support the {@code add} or
     * {@code addAll} operations.
     *
     * <p>The view's {@code iterator} is a "weakly consistent" iterator that
     * will never throw {@link java.util.ConcurrentModificationException}, and
     * guarantees to traverse elements as they existed upon construction of the
     * iterator, and may (but is not guaranteed to) reflect any modifications
     * subsequent to construction.
     */
    @Override public Set<Entry<K, V>> entrySet() {
      Set<Entry<K, V>> es = entrySet;
      return (es != null) ? es : (entrySet = new EntrySet());
    }

    /* ---------------- Iterator Support -------------- */

    abstract class HashIterator {

      int nextSegmentIndex;
      int nextTableIndex;
      AtomicReferenceArray<E> currentTable;
      E nextEntry;
      WriteThroughEntry nextExternal;
      WriteThroughEntry lastReturned;

      HashIterator() {
        nextSegmentIndex = segments.length - 1;
        nextTableIndex = -1;
        advance();
      }

      public boolean hasMoreElements() {
        return hasNext();
      }

      final void advance() {
        nextExternal = null;

        if (nextInChain()) {
          return;
        }

        if (nextInTable()) {
          return;
        }

        while (nextSegmentIndex >= 0) {
          Segment seg = segments[nextSegmentIndex--];
          if (seg.count != 0) {
            currentTable = seg.table;
            nextTableIndex = currentTable.length() - 1;
            if (nextInTable()) {
              return;
            }
          }
        }
      }

      /**
       * Finds the next entry in the current chain. Returns true if an entry
       * was found.
       */
      boolean nextInChain() {
        Strategy<K, V, E> s = Impl.this.strategy;
        if (nextEntry != null) {
          for (nextEntry = s.getNext(nextEntry); nextEntry != null;
              nextEntry = s.getNext(nextEntry)) {
            if (advanceTo(nextEntry)) {
              return true;
            }
          }
        }
        return false;
      }

      /**
       * Finds the next entry in the current table. Returns true if an entry
       * was found.
       */
      boolean nextInTable() {
        while (nextTableIndex >= 0) {
          if ((nextEntry = currentTable.get(nextTableIndex--)) != null) {
            if (advanceTo(nextEntry) || nextInChain()) {
              return true;
            }
          }
        }
        return false;
      }

      /**
       * Advances to the given entry. Returns true if the entry was valid,
       * false if it should be skipped.
       */
      boolean advanceTo(E entry) {
        Strategy<K, V, E> s = Impl.this.strategy;
        K key = s.getKey(entry);
        V value = s.getValue(entry);
        if (key != null && value != null) {
          nextExternal = new WriteThroughEntry(key, value);
          return true;
        } else {
          // Skip partially reclaimed entry.
          return false;
        }
      }

      public boolean hasNext() {
        return nextExternal != null;
      }

      WriteThroughEntry nextEntry() {
        if (nextExternal == null) {
          throw new NoSuchElementException();
        }
        lastReturned = nextExternal;
        advance();
        return lastReturned;
      }

      public void remove() {
        if (lastReturned == null) {
          throw new IllegalStateException();
        }
        Impl.this.remove(lastReturned.getKey());
        lastReturned = null;
      }
    }

    final class KeyIterator extends HashIterator implements Iterator<K> {

      public K next() {
        return super.nextEntry().getKey();
      }
    }

    final class ValueIterator extends HashIterator implements Iterator<V> {

      public V next() {
        return super.nextEntry().getValue();
      }
    }

    /**
     * Custom Entry class used by EntryIterator.next(), that relays setValue
     * changes to the underlying map.
     */
    final class WriteThroughEntry extends AbstractMapEntry<K, V> {
      final K key;
      V value;

      WriteThroughEntry(K key, V value) {
        this.key = key;
        this.value = value;
      }

      @Override public K getKey() {
        return key;
      }

      @Override public V getValue() {
        return value;
      }

      /**
       * Set our entry's value and write through to the map. The value to
       * return is somewhat arbitrary here. Since a WriteThroughEntry does not
       * necessarily track asynchronous changes, the most recent "previous"
       * value could be different from what we return (or could even have been
       * removed in which case the put will re-establish). We do not and
       * cannot guarantee more.
       */
      @Override public V setValue(V value) {
        if (value == null) {
          throw new NullPointerException();
        }
        V oldValue = Impl.this.put(getKey(), value);
        this.value = value;
        return oldValue;
      }
    }

    final class EntryIterator extends HashIterator
        implements Iterator<Entry<K, V>> {

      public Entry<K, V> next() {
        return nextEntry();
      }
    }

    final class KeySet extends AbstractSet<K> {

      @Override public Iterator<K> iterator() {
        return new KeyIterator();
      }

      @Override public int size() {
        return Impl.this.size();
      }

      @Override public boolean isEmpty() {
        return Impl.this.isEmpty();
      }

      @Override public boolean contains(Object o) {
        return Impl.this.containsKey(o);
      }

      @Override public boolean remove(Object o) {
        return Impl.this.remove(o) != null;
      }

      @Override public void clear() {
        Impl.this.clear();
      }
    }

    final class Values extends AbstractCollection<V> {

      @Override public Iterator<V> iterator() {
        return new ValueIterator();
      }

      @Override public int size() {
        return Impl.this.size();
      }

      @Override public boolean isEmpty() {
        return Impl.this.isEmpty();
      }

      @Override public boolean contains(Object o) {
        return Impl.this.containsValue(o);
      }

      @Override public void clear() {
        Impl.this.clear();
      }
    }

    final class EntrySet extends AbstractSet<Entry<K, V>> {

      @Override public Iterator<Entry<K, V>> iterator() {
        return new EntryIterator();
      }

      @Override public boolean contains(Object o) {
        if (!(o instanceof Entry)) {
          return false;
        }
        Entry<?, ?> e = (Entry<?, ?>) o;
        Object key = e.getKey();
        if (key == null) {
          return false;
        }
        V v = Impl.this.get(key);

        return v != null && strategy.equalValues(v, e.getValue());
      }

      @Override public boolean remove(Object o) {
        if (!(o instanceof Entry)) {
          return false;
        }
        Entry<?, ?> e = (Entry<?, ?>) o;
        Object key = e.getKey();
        return key != null && Impl.this.remove(key, e.getValue());
      }

      @Override public int size() {
        return Impl.this.size();
      }

      @Override public boolean isEmpty() {
        return Impl.this.isEmpty();
      }

      @Override public void clear() {
        Impl.this.clear();
      }
    }

    /* ---------------- Serialization Support -------------- */

    private static final long serialVersionUID = 1;

    private void writeObject(java.io.ObjectOutputStream out)
        throws IOException {
      out.writeInt(size());
      out.writeInt(segments.length); // concurrencyLevel
      out.writeObject(strategy);
      for (Entry<K, V> entry : entrySet()) {
        out.writeObject(entry.getKey());
        out.writeObject(entry.getValue());
      }
      out.writeObject(null); // terminate entries
    }

    /**
     * Fields used during deserialization. We use a nested class so we don't
     * load them until we need them. We need to use reflection to set final
     * fields outside of the constructor.
     */
    static class Fields {

      static final Field segmentShift = findField("segmentShift");
      static final Field segmentMask = findField("segmentMask");
      static final Field segments = findField("segments");
      static final Field strategy = findField("strategy");

      static Field findField(String name) {
        try {
          Field f = Impl.class.getDeclaredField(name);
          f.setAccessible(true);
          return f;
        } catch (NoSuchFieldException e) {
          throw new AssertionError(e);
        }
      }
    }

    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      try {
        int initialCapacity = in.readInt();
        int concurrencyLevel = in.readInt();
        Strategy<K, V, E> strategy = (Strategy<K, V, E>) in.readObject();

        if (concurrencyLevel > MAX_SEGMENTS) {
          concurrencyLevel = MAX_SEGMENTS;
        }

        // Find power-of-two sizes best matching arguments
        int segmentShift = 0;
        int segmentCount = 1;
        while (segmentCount < concurrencyLevel) {
          ++segmentShift;
          segmentCount <<= 1;
        }
        Fields.segmentShift.set(this, 32 - segmentShift);
        Fields.segmentMask.set(this, segmentCount - 1);
        Fields.segments.set(this, newSegmentArray(segmentCount));

        if (initialCapacity > MAXIMUM_CAPACITY) {
          initialCapacity = MAXIMUM_CAPACITY;
        }

        int segmentCapacity = initialCapacity / segmentCount;
        if (segmentCapacity * segmentCount < initialCapacity) {
          ++segmentCapacity;
        }

        int segmentSize = 1;
        while (segmentSize < segmentCapacity) {
            segmentSize <<= 1;
        }
        for (int i = 0; i < this.segments.length; ++i) {
          this.segments[i] = new Segment(segmentSize);
        }

        Fields.strategy.set(this, strategy);

        while (true) {
          K key = (K) in.readObject();
          if (key == null) {
            break; // terminator
          }
          V value = (V) in.readObject();
          put(key, value);
        }
      } catch (IllegalAccessException e) {
        throw new AssertionError(e);
      }
    }
  }

  static class ComputingImpl<K, V, E> extends Impl<K, V, E> {

    static final long serialVersionUID = 0;

    final ComputingStrategy<K, V, E> computingStrategy;
    final Function<? super K, ? extends V> computer;

    /**
     * Creates a new, empty map with the specified strategy, initial capacity,
     * load factor and concurrency level.
     */
    ComputingImpl(ComputingStrategy<K, V, E> strategy, Builder builder,
        Function<? super K, ? extends V> computer) {
      super(strategy, builder);
      this.computingStrategy = strategy;
      this.computer = computer;
    }

    @Override public V get(Object k) {
      /*
       * This cast isn't safe, but we can rely on the fact that K is almost
       * always passed to Map.get(), and tools like IDEs and Findbugs can
       * catch situations where this isn't the case.
       *
       * The alternative is to add an overloaded method, but the chances of
       * a user calling get() instead of the new API and the risks inherent
       * in adding a new API outweigh this little hole.
       */
      @SuppressWarnings("unchecked")
      K key = (K) k;

      if (key == null) {
        throw new NullPointerException("key");
      }

      int hash = hash(key);
      Segment segment = segmentFor(hash);
      outer: while (true) {
        E entry = segment.getEntry(key, hash);
        if (entry == null) {
          boolean created = false;
          segment.lock();
          try {
            // Try again--an entry could have materialized in the interim.
            entry = segment.getEntry(key, hash);
            if (entry == null) {
              // Create a new entry.
              created = true;
              int count = segment.count;
              if (count++ > segment.threshold) { // ensure capacity
                segment.expand();
              }
              AtomicReferenceArray<E> table = segment.table;
              int index = hash & (table.length() - 1);
              E first = table.get(index);
              ++segment.modCount;
              entry = computingStrategy.newEntry(key, hash, first);
              table.set(index, entry);
              segment.count = count; // write-volatile
            }
          } finally {
            segment.unlock();
          }

          if (created) {
            // This thread solely created the entry.
            boolean success = false;
            try {
              V value = computingStrategy.compute(key, entry, computer);
              if (value == null) {
                throw new NullPointerException(
                    "compute() returned null unexpectedly");
              }
              success = true;
              return value;
            } finally {
              if (!success) {
                segment.removeEntry(entry, hash);
              }
            }
          }
        }

        // The entry already exists. Wait for the computation.
        boolean interrupted = false;
        try {
          while (true) {
            try {
              V value = computingStrategy.waitForValue(entry);
              if (value == null) {
                // Purge entry and try again.
                segment.removeEntry(entry, hash);
                continue outer;
              }
              return value;
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  /**
   * A basic, no-frills implementation of {@code Strategy} using {@link
   * SimpleInternalEntry}. Intended to be subclassed to provide customized
   * behavior. For example, the following creates a map that uses byte arrays as
   * keys: <pre>   {@code
   *
   *   return new CustomConcurrentHashMap.Builder().buildMap(
   *       new SimpleStrategy<byte[], V>() {
   *         public int hashKey(Object key) {
   *           return Arrays.hashCode((byte[]) key);
   *         }
   *         public boolean equalKeys(byte[] a, Object b) {
   *           return Arrays.equals(a, (byte[]) b);
   *         }
   *       });}</pre>
   *
   * With nothing overridden, it yields map behavior equivalent to that of
   * {@link ConcurrentHashMap}.
   */
  static class SimpleStrategy<K, V>
      implements Strategy<K, V, SimpleInternalEntry<K, V>> {
    public SimpleInternalEntry<K, V> newEntry(
        K key, int hash, SimpleInternalEntry<K, V> next) {
      return new SimpleInternalEntry<K, V>(key, hash, null, next);
    }
    public SimpleInternalEntry<K, V> copyEntry(K key,
        SimpleInternalEntry<K, V> original, SimpleInternalEntry<K, V> next) {
      return new SimpleInternalEntry<K, V>(
          key, original.hash, original.value, next);
    }
    public void setValue(SimpleInternalEntry<K, V> entry, V value) {
      entry.value = value;
    }
    public V getValue(SimpleInternalEntry<K, V> entry) {
      return entry.value;
    }
    public boolean equalKeys(K a, Object b) {
      return a.equals(b);
    }
    public boolean equalValues(V a, Object b) {
      return a.equals(b);
    }
    public int hashKey(Object key) {
      return key.hashCode();
    }
    public K getKey(SimpleInternalEntry<K, V> entry) {
      return entry.key;
    }
    public SimpleInternalEntry<K, V> getNext(SimpleInternalEntry<K, V> entry) {
      return entry.next;
    }
    public int getHash(SimpleInternalEntry<K, V> entry) {
      return entry.hash;
    }
    public void setInternals(
        Internals<K, V, SimpleInternalEntry<K, V>> internals) {
      // ignore?
    }
  }

  /**
   * A basic, no-frills entry class used by {@link SimpleInternalEntry}.
   */
  static class SimpleInternalEntry<K, V> {
    final K key;
    final int hash;
    final SimpleInternalEntry<K, V> next;
    volatile V value;
    SimpleInternalEntry(
        K key, int hash, @Nullable V value, SimpleInternalEntry<K, V> next) {
      this.key = key;
      this.hash = hash;
      this.value = value;
      this.next = next;
    }
  }
}
