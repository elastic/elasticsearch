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
import org.elasticsearch.util.annotations.GwtIncompatible;
import org.elasticsearch.util.base.FinalizableReferenceQueue;
import org.elasticsearch.util.base.FinalizableSoftReference;
import org.elasticsearch.util.base.FinalizableWeakReference;
import org.elasticsearch.util.base.Function;
import org.elasticsearch.util.collect.CustomConcurrentHashMap.ComputingStrategy;
import org.elasticsearch.util.collect.CustomConcurrentHashMap.Internals;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ConcurrentMap} builder, providing any combination of these
 * features: {@linkplain SoftReference soft} or {@linkplain WeakReference
 * weak} keys, soft or weak values, timed expiration, and on-demand
 * computation of values. Usage example: <pre> {@code
 *
 *   ConcurrentMap<Key, Graph> graphs = new MapMaker()
 *       .concurrencyLevel(32)
 *       .softKeys()
 *       .weakValues()
 *       .expiration(30, TimeUnit.MINUTES)
 *       .makeComputingMap(
 *           new Function<Key, Graph>() {
 *             public Graph apply(Key key) {
 *               return createExpensiveGraph(key);
 *             }
 *           });}</pre>
 *
 * These features are all optional; {@code new MapMaker().makeMap()}
 * returns a valid concurrent map that behaves exactly like a
 * {@link ConcurrentHashMap}.
 *
 * The returned map is implemented as a hash table with similar performance
 * characteristics to {@link ConcurrentHashMap}. It supports all optional
 * operations of the {@code ConcurrentMap} interface. It does not permit
 * null keys or values. It is serializable; however, serializing a map that
 * uses soft or weak references can give unpredictable results.
 *
 * <p><b>Note:</b> by default, the returned map uses equality comparisons
 * (the {@link Object#equals(Object) equals} method) to determine equality
 * for keys or values. However, if {@link #weakKeys()} or {@link
 * #softKeys()} was specified, the map uses identity ({@code ==})
 * comparisons instead for keys. Likewise, if {@link #weakValues()} or
 * {@link #softValues()} was specified, the map uses identity comparisons
 * for values.
 *
 * <p>The returned map has <i>weakly consistent iteration</i>: an iterator
 * over one of the map's view collections may reflect some, all or none of
 * the changes made to the map after the iterator was created.
 *
 * <p>An entry whose key or value is reclaimed by the garbage collector
 * immediately disappears from the map. (If the default settings of strong
 * keys and strong values are used, this will never happen.) The client can
 * never observe a partially-reclaimed entry. Any {@link java.util.Map.Entry}
 * instance retrieved from the map's {@linkplain Map#entrySet() entry set}
 * is snapshot of that entry's state at the time of retrieval.
 *
 * <p>{@code new MapMaker().weakKeys().makeMap()} can almost always be
 * used as a drop-in replacement for {@link java.util.WeakHashMap}, adding
 * concurrency, asynchronous cleanup, identity-based equality for keys, and
 * great flexibility.
 *
 * @author Bob Lee
 * @author Kevin Bourrillion
 */
@GwtCompatible(emulated = true)
public final class MapMaker {
  private Strength keyStrength = Strength.STRONG;
  private Strength valueStrength = Strength.STRONG;
  private long expirationNanos = 0;
  private boolean useCustomMap;
  private final CustomConcurrentHashMap.Builder builder
      = new CustomConcurrentHashMap.Builder();

  /**
   * Constructs a new {@code MapMaker} instance with default settings,
   * including strong keys, strong values, and no automatic expiration.
   */
  public MapMaker() {}

  /**
   * Sets a custom initial capacity (defaults to 16). Resizing this or
   * any other kind of hash table is a relatively slow operation, so,
   * when possible, it is a good idea to provide estimates of expected
   * table sizes.
   *
   * @throws IllegalArgumentException if {@code initialCapacity} is
   *   negative
   * @throws IllegalStateException if an initial capacity was already set
   */
  public MapMaker initialCapacity(int initialCapacity) {
    builder.initialCapacity(initialCapacity);
    return this;
  }

  /**
   * Guides the allowed concurrency among update operations. Used as a
   * hint for internal sizing. The table is internally partitioned to try
   * to permit the indicated number of concurrent updates without
   * contention.  Because placement in hash tables is essentially random,
   * the actual concurrency will vary. Ideally, you should choose a value
   * to accommodate as many threads as will ever concurrently modify the
   * table. Using a significantly higher value than you need can waste
   * space and time, and a significantly lower value can lead to thread
   * contention. But overestimates and underestimates within an order of
   * magnitude do not usually have much noticeable impact. A value of one
   * is appropriate when it is known that only one thread will modify and
   * all others will only read. Defaults to 16.
   *
   * @throws IllegalArgumentException if {@code concurrencyLevel} is
   *     nonpositive
   * @throws IllegalStateException if a concurrency level was already set
   */
  @GwtIncompatible("java.util.concurrent.ConcurrentHashMap concurrencyLevel")
  public MapMaker concurrencyLevel(int concurrencyLevel) {
    builder.concurrencyLevel(concurrencyLevel);
    return this;
  }

  /**
   * Specifies that each key (not value) stored in the map should be
   * wrapped in a {@link WeakReference} (by default, strong references
   * are used).
   *
   * <p><b>Note:</b> the map will use identity ({@code ==}) comparison
   * to determine equality of weak keys, which may not behave as you expect.
   * For example, storing a key in the map and then attempting a lookup
   * using a different but {@link Object#equals(Object) equals}-equivalent
   * key will always fail.
   *
   * @throws IllegalStateException if the key strength was already set
   * @see WeakReference
   */
  @GwtIncompatible("java.lang.ref.WeakReference")
  public MapMaker weakKeys() {
    return setKeyStrength(Strength.WEAK);
  }

  /**
   * Specifies that each key (not value) stored in the map should be
   * wrapped in a {@link SoftReference} (by default, strong references
   * are used).
   *
   * <p><b>Note:</b> the map will use identity ({@code ==}) comparison
   * to determine equality of soft keys, which may not behave as you expect.
   * For example, storing a key in the map and then attempting a lookup
   * using a different but {@link Object#equals(Object) equals}-equivalent
   * key will always fail.
   *
   * @throws IllegalStateException if the key strength was already set
   * @see SoftReference
   */
  @GwtIncompatible("java.lang.ref.SoftReference")
  public MapMaker softKeys() {
    return setKeyStrength(Strength.SOFT);
  }

  private MapMaker setKeyStrength(Strength strength) {
    if (keyStrength != Strength.STRONG) {
      throw new IllegalStateException("Key strength was already set to "
          + keyStrength + ".");
    }
    keyStrength = strength;
    useCustomMap = true;
    return this;
  }

  /**
   * Specifies that each value (not key) stored in the map should be
   * wrapped in a {@link WeakReference} (by default, strong references
   * are used).
   *
   * <p>Weak values will be garbage collected once they are weakly
   * reachable. This makes them a poor candidate for caching; consider
   * {@link #softValues()} instead.
   *
   * <p><b>Note:</b> the map will use identity ({@code ==}) comparison
   * to determine equality of weak values. This will notably impact
   * the behavior of {@link Map#containsValue(Object) containsValue},
   * {@link ConcurrentMap#remove(Object, Object) remove(Object, Object)},
   * and {@link ConcurrentMap#replace(Object, Object, Object) replace(K, V, V)}.
   *
   * @throws IllegalStateException if the key strength was already set
   * @see WeakReference
   */
  @GwtIncompatible("java.lang.ref.WeakReference")
  public MapMaker weakValues() {
    return setValueStrength(Strength.WEAK);
  }

  /**
   * Specifies that each value (not key) stored in the map should be
   * wrapped in a {@link SoftReference} (by default, strong references
   * are used).
   *
   * <p>Soft values will be garbage collected in response to memory
   * demand, and in a least-recently-used manner. This makes them a
   * good candidate for caching.
   *
   * <p><b>Note:</b> the map will use identity ({@code ==}) comparison
   * to determine equality of soft values. This will notably impact
   * the behavior of {@link Map#containsValue(Object) containsValue},
   * {@link ConcurrentMap#remove(Object, Object) remove(Object, Object)},
   * and {@link ConcurrentMap#replace(Object, Object, Object) replace(K, V, V)}.
   *
   * @throws IllegalStateException if the value strength was already set
   * @see SoftReference
   */
  @GwtIncompatible("java.lang.ref.SoftReference")
  public MapMaker softValues() {
    return setValueStrength(Strength.SOFT);
  }

  private MapMaker setValueStrength(Strength strength) {
    if (valueStrength != Strength.STRONG) {
      throw new IllegalStateException("Value strength was already set to "
          + valueStrength + ".");
    }
    valueStrength = strength;
    useCustomMap = true;
    return this;
  }

  /**
   * Specifies that each entry should be automatically removed from the
   * map once a fixed duration has passed since the entry's creation.
   *
   * @param duration the length of time after an entry is created that it
   *     should be automatically removed
   * @param unit the unit that {@code duration} is expressed in
   * @throws IllegalArgumentException if {@code duration} is not positive
   * @throws IllegalStateException if the expiration time was already set
   */
  public MapMaker expiration(long duration, TimeUnit unit) {
    if (expirationNanos != 0) {
      throw new IllegalStateException("expiration time of "
          + expirationNanos + " ns was already set");
    }
    if (duration <= 0) {
      throw new IllegalArgumentException("invalid duration: " + duration);
    }
    this.expirationNanos = unit.toNanos(duration);
    useCustomMap = true;
    return this;
  }

  /**
   * Builds the final map, without on-demand computation of values. This method
   * does not alter the state of this {@code MapMaker} instance, so it can be
   * invoked again to create multiple independent maps.
   *
   * @param <K> the type of keys to be stored in the returned map
   * @param <V> the type of values to be stored in the returned map
   * @return a concurrent map having the requested features
   */
  public <K, V> ConcurrentMap<K, V> makeMap() {
    return useCustomMap
        ? new StrategyImpl<K, V>(this).map
        : new ConcurrentHashMap<K, V>(builder.getInitialCapacity(),
            0.75f, builder.getConcurrencyLevel());
  }

  /**
   * Builds a map that supports atomic, on-demand computation of values. {@link
   * Map#get} either returns an already-computed value for the given key,
   * atomically computes it using the supplied function, or, if another thread
   * is currently computing the value for this key, simply waits for that thread
   * to finish and returns its computed value. Note that the function may be
   * executed concurrently by multiple threads, but only for distinct keys.
   *
   * <p>If an entry's value has not finished computing yet, query methods
   * besides {@code get} return immediately as if an entry doesn't exist. In
   * other words, an entry isn't externally visible until the value's
   * computation completes.
   *
   * <p>{@link Map#get} on the returned map will never return {@code null}. It
   * may throw:
   *
   * <ul>
   * <li>{@link NullPointerException} if the key is null or the computing
   *     function returns null
   * <li>{@link ComputationException} if an exception was thrown by the
   *     computing function. If that exception is already of type {@link
   *     ComputationException}, it is propagated directly; otherwise it is
   *     wrapped.
   * </ul>
   *
   * <p><b>Note:</b> Callers of {@code get} <i>must</i> ensure that the key
   * argument is of type {@code K}. The {@code get} method accepts {@code
   * Object}, so the key type is not checked at compile time. Passing an object
   * of a type other than {@code K} can result in that object being unsafely
   * passed to the computing function as type {@code K}, and unsafely stored in
   * the map.
   *
   * <p>If {@link Map#put} is called before a computation completes, other
   * threads waiting on the computation will wake up and return the stored
   * value. When the computation completes, its new result will overwrite the
   * value that was put in the map manually.
   *
   * <p>This method does not alter the state of this {@code MapMaker} instance,
   * so it can be invoked again to create multiple independent maps.
   */
  public <K, V> ConcurrentMap<K, V> makeComputingMap(
      Function<? super K, ? extends V> computingFunction) {
    return new StrategyImpl<K, V>(this, computingFunction).map;
  }

  // Remainder of this file is private implementation details

  private enum Strength {
    WEAK {
      @Override boolean equal(Object a, Object b) {
        return a == b;
      }
      @Override int hash(Object o) {
        return System.identityHashCode(o);
      }
      @Override <K, V> ValueReference<K, V> referenceValue(
          ReferenceEntry<K, V> entry, V value) {
        return new WeakValueReference<K, V>(value, entry);
      }
      @Override <K, V> ReferenceEntry<K, V> newEntry(
          Internals<K, V, ReferenceEntry<K, V>> internals, K key,
          int hash, ReferenceEntry<K, V> next) {
        return (next == null)
            ? new WeakEntry<K, V>(internals, key, hash)
            : new LinkedWeakEntry<K, V>(internals, key, hash, next);
      }
      @Override <K, V> ReferenceEntry<K, V> copyEntry(
          K key, ReferenceEntry<K, V> original,
          ReferenceEntry<K, V> newNext) {
        WeakEntry<K, V> from = (WeakEntry<K, V>) original;
        return (newNext == null)
            ? new WeakEntry<K, V>(from.internals, key, from.hash)
            : new LinkedWeakEntry<K, V>(
                from.internals, key, from.hash, newNext);
      }
    },

    SOFT {
      @Override boolean equal(Object a, Object b) {
        return a == b;
      }
      @Override int hash(Object o) {
        return System.identityHashCode(o);
      }
      @Override <K, V> ValueReference<K, V> referenceValue(
          ReferenceEntry<K, V> entry, V value) {
        return new SoftValueReference<K, V>(value, entry);
      }
      @Override <K, V> ReferenceEntry<K, V> newEntry(
          Internals<K, V, ReferenceEntry<K, V>> internals, K key,
          int hash, ReferenceEntry<K, V> next) {
        return (next == null)
            ? new SoftEntry<K, V>(internals, key, hash)
            : new LinkedSoftEntry<K, V>(internals, key, hash, next);
      }
      @Override <K, V> ReferenceEntry<K, V> copyEntry(
          K key, ReferenceEntry<K, V> original,
          ReferenceEntry<K, V> newNext) {
        SoftEntry<K, V> from = (SoftEntry<K, V>) original;
        return (newNext == null)
            ? new SoftEntry<K, V>(from.internals, key, from.hash)
            : new LinkedSoftEntry<K, V>(
                from.internals, key, from.hash, newNext);
      }
    },

    STRONG {
      @Override boolean equal(Object a, Object b) {
        return a.equals(b);
      }
      @Override int hash(Object o) {
        return o.hashCode();
      }
      @Override <K, V> ValueReference<K, V> referenceValue(
          ReferenceEntry<K, V> entry, V value) {
        return new StrongValueReference<K, V>(value);
      }
      @Override <K, V> ReferenceEntry<K, V> newEntry(
          Internals<K, V, ReferenceEntry<K, V>> internals, K key,
          int hash, ReferenceEntry<K, V> next) {
        return (next == null)
            ? new StrongEntry<K, V>(internals, key, hash)
            : new LinkedStrongEntry<K, V>(
                internals, key, hash, next);
      }
      @Override <K, V> ReferenceEntry<K, V> copyEntry(
          K key, ReferenceEntry<K, V> original,
          ReferenceEntry<K, V> newNext) {
        StrongEntry<K, V> from = (StrongEntry<K, V>) original;
        return (newNext == null)
            ? new StrongEntry<K, V>(from.internals, key, from.hash)
            : new LinkedStrongEntry<K, V>(
                from.internals, key, from.hash, newNext);
      }
    };

    /**
     * Determines if two keys or values are equal according to this
     * strength strategy.
     */
    abstract boolean equal(Object a, Object b);

    /**
     * Hashes a key according to this strategy.
     */
    abstract int hash(Object o);

    /**
     * Creates a reference for the given value according to this value
     * strength.
     */
    abstract <K, V> ValueReference<K, V> referenceValue(
        ReferenceEntry<K, V> entry, V value);

    /**
     * Creates a new entry based on the current key strength.
     */
    abstract <K, V> ReferenceEntry<K, V> newEntry(
        Internals<K, V, ReferenceEntry<K, V>> internals, K key,
        int hash, ReferenceEntry<K, V> next);

    /**
     * Creates a new entry and copies the value and other state from an
     * existing entry.
     */
    abstract <K, V> ReferenceEntry<K, V> copyEntry(K key,
        ReferenceEntry<K, V> original, ReferenceEntry<K, V> newNext);
  }

  private static class StrategyImpl<K, V> implements Serializable,
      ComputingStrategy<K, V, ReferenceEntry<K, V>> {
    final Strength keyStrength;
    final Strength valueStrength;
    final ConcurrentMap<K, V> map;
    final long expirationNanos;
    Internals<K, V, ReferenceEntry<K, V>> internals;

    StrategyImpl(MapMaker maker) {
      this.keyStrength = maker.keyStrength;
      this.valueStrength = maker.valueStrength;
      this.expirationNanos = maker.expirationNanos;

      map = maker.builder.buildMap(this);
    }

    StrategyImpl(
        MapMaker maker, Function<? super K, ? extends V> computer) {
      this.keyStrength = maker.keyStrength;
      this.valueStrength = maker.valueStrength;
      this.expirationNanos = maker.expirationNanos;

      map = maker.builder.buildComputingMap(this, computer);
    }

    public void setValue(ReferenceEntry<K, V> entry, V value) {
      setValueReference(
          entry, valueStrength.referenceValue(entry, value));
      if (expirationNanos > 0) {
        scheduleRemoval(entry.getKey(), value);
      }
    }

    void scheduleRemoval(K key, V value) {
      /*
       * TODO: Keep weak reference to map, too. Build a priority
       * queue out of the entries themselves instead of creating a
       * task per entry. Then, we could have one recurring task per
       * map (which would clean the entire map and then reschedule
       * itself depending upon when the next expiration comes). We
       * also want to avoid removing an entry prematurely if the
       * entry was set to the same value again.
       */
      final WeakReference<K> keyReference = new WeakReference<K>(key);
      final WeakReference<V> valueReference = new WeakReference<V>(value);
      ExpirationTimer.instance.schedule(
          new TimerTask() {
            @Override public void run() {
              K key = keyReference.get();
              if (key != null) {
                // Remove if the value is still the same.
                map.remove(key, valueReference.get());
              }
            }
          }, TimeUnit.NANOSECONDS.toMillis(expirationNanos));
    }

    public boolean equalKeys(K a, Object b) {
      return keyStrength.equal(a, b);
    }

    public boolean equalValues(V a, Object b) {
      return valueStrength.equal(a, b);
    }

    public int hashKey(Object key) {
      return keyStrength.hash(key);
    }

    public K getKey(ReferenceEntry<K, V> entry) {
      return entry.getKey();
    }

    public int getHash(ReferenceEntry<K, V> entry) {
      return entry.getHash();
    }

    public ReferenceEntry<K, V> newEntry(
        K key, int hash, ReferenceEntry<K, V> next) {
      return keyStrength.newEntry(internals, key, hash, next);
    }

    public ReferenceEntry<K, V> copyEntry(K key,
        ReferenceEntry<K, V> original, ReferenceEntry<K, V> newNext) {
      ValueReference<K, V> valueReference = original.getValueReference();
      if (valueReference == COMPUTING) {
        ReferenceEntry<K, V> newEntry
            = newEntry(key, original.getHash(), newNext);
        newEntry.setValueReference(
            new FutureValueReference(original, newEntry));
        return newEntry;
      } else {
        ReferenceEntry<K, V> newEntry
            = newEntry(key, original.getHash(), newNext);
        newEntry.setValueReference(valueReference.copyFor(newEntry));
        return newEntry;
      }
    }

    /**
     * Waits for a computation to complete. Returns the result of the
     * computation or null if none was available.
     */
    public V waitForValue(ReferenceEntry<K, V> entry)
        throws InterruptedException {
      ValueReference<K, V> valueReference = entry.getValueReference();
      if (valueReference == COMPUTING) {
        synchronized (entry) {
          while ((valueReference = entry.getValueReference())
              == COMPUTING) {
            entry.wait();
          }
        }
      }
      return valueReference.waitForValue();
    }

    /**
     * Used by CustomConcurrentHashMap to retrieve values. Returns null
     * instead of blocking or throwing an exception.
     */
    public V getValue(ReferenceEntry<K, V> entry) {
      ValueReference<K, V> valueReference = entry.getValueReference();
      return valueReference.get();
    }

    public V compute(K key, final ReferenceEntry<K, V> entry,
        Function<? super K, ? extends V> computer) {
      V value;
      try {
        value = computer.apply(key);
      } catch (ComputationException e) {
        // if computer has thrown a computation exception, propagate rather
        // than wrap
        setValueReference(entry,
            new ComputationExceptionReference<K, V>(e.getCause()));
        throw e;
      } catch (Throwable t) {
        setValueReference(
          entry, new ComputationExceptionReference<K, V>(t));
        throw new ComputationException(t);
      }

      if (value == null) {
        String message
            = computer + " returned null for key " + key + ".";
        setValueReference(
            entry, new NullOutputExceptionReference<K, V>(message));
        throw new NullOutputException(message);
      } else {
        setValue(entry, value);
      }
      return value;
    }

    /**
     * Sets the value reference on an entry and notifies waiting
     * threads.
     */
    void setValueReference(ReferenceEntry<K, V> entry,
        ValueReference<K, V> valueReference) {
      boolean notifyOthers = (entry.getValueReference() == COMPUTING);
      entry.setValueReference(valueReference);
      if (notifyOthers) {
        synchronized (entry) {
          entry.notifyAll();
        }
      }
    }

    /**
     * Points to an old entry where a value is being computed. Used to
     * support non-blocking copying of entries during table expansion,
     * removals, etc.
     */
    private class FutureValueReference implements ValueReference<K, V> {
      final ReferenceEntry<K, V> original;
      final ReferenceEntry<K, V> newEntry;

      FutureValueReference(
          ReferenceEntry<K, V> original, ReferenceEntry<K, V> newEntry) {
        this.original = original;
        this.newEntry = newEntry;
      }

      public V get() {
        boolean success = false;
        try {
          V value = original.getValueReference().get();
          success = true;
          return value;
        } finally {
          if (!success) {
            removeEntry();
          }
        }
      }

      public ValueReference<K, V> copyFor(ReferenceEntry<K, V> entry) {
        return new FutureValueReference(original, entry);
      }

      public V waitForValue() throws InterruptedException {
        boolean success = false;
        try {
          // assert that key != null
          V value = StrategyImpl.this.waitForValue(original);
          success = true;
          return value;
        } finally {
          if (!success) {
            removeEntry();
          }
        }
      }

      /**
       * Removes the entry in the event of an exception. Ideally,
       * we'd clean up as soon as the computation completes, but we
       * can't do that without keeping a reference to this entry from
       * the original.
       */
      void removeEntry() {
        internals.removeEntry(newEntry);
      }
    }

    public ReferenceEntry<K, V> getNext(
        ReferenceEntry<K, V> entry) {
      return entry.getNext();
    }

    public void setInternals(
        Internals<K, V, ReferenceEntry<K, V>> internals) {
      this.internals = internals;
    }

    private static final long serialVersionUID = 0;

    private void writeObject(ObjectOutputStream out)
        throws IOException {
      // Custom serialization code ensures that the key and value
      // strengths are written before the map. We'll need them to
      // deserialize the map entries.
      out.writeObject(keyStrength);
      out.writeObject(valueStrength);
      out.writeLong(expirationNanos);

      // TODO: It is possible for the strategy to try to use the map
      // or internals during deserialization, for example, if an
      // entry gets reclaimed. We could detect this case and queue up
      // removals to be flushed after we deserialize the map.
      out.writeObject(internals);
      out.writeObject(map);
    }

    /**
     * Fields used during deserialization. We use a nested class so we
     * don't load them until we need them. We need to use reflection to
     * set final fields outside of the constructor.
     */
    private static class Fields {
      static final Field keyStrength = findField("keyStrength");
      static final Field valueStrength = findField("valueStrength");
      static final Field expirationNanos = findField("expirationNanos");
      static final Field internals = findField("internals");
      static final Field map = findField("map");

      static Field findField(String name) {
        try {
          Field f = StrategyImpl.class.getDeclaredField(name);
          f.setAccessible(true);
          return f;
        } catch (NoSuchFieldException e) {
          throw new AssertionError(e);
        }
      }
    }

    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      try {
        Fields.keyStrength.set(this, in.readObject());
        Fields.valueStrength.set(this, in.readObject());
        Fields.expirationNanos.set(this, in.readLong());
        Fields.internals.set(this, in.readObject());
        Fields.map.set(this, in.readObject());
      } catch (IllegalAccessException e) {
        throw new AssertionError(e);
      }
    }
  }

  /** A reference to a value. */
  private interface ValueReference<K, V> {
    /**
     * Gets the value. Does not block or throw exceptions.
     */
    V get();

    /** Creates a copy of this reference for the given entry. */
    ValueReference<K, V> copyFor(ReferenceEntry<K, V> entry);

    /**
     * Waits for a value that may still be computing. Unlike get(),
     * this method can block (in the case of FutureValueReference) or
     * throw an exception.
     */
    V waitForValue() throws InterruptedException;
  }

  private static final ValueReference<Object, Object> COMPUTING
      = new ValueReference<Object, Object>() {
    public Object get() {
      return null;
    }
    public ValueReference<Object, Object> copyFor(
        ReferenceEntry<Object, Object> entry) {
      throw new AssertionError();
    }
    public Object waitForValue() {
      throw new AssertionError();
    }
  };

  /**
   * Singleton placeholder that indicates a value is being computed.
   */
  @SuppressWarnings("unchecked")
  // Safe because impl never uses a parameter or returns any non-null value
  private static <K, V> ValueReference<K, V> computing() {
    return (ValueReference<K, V>) COMPUTING;
  }

  /** Used to provide null output exceptions to other threads. */
  private static class NullOutputExceptionReference<K, V>
      implements ValueReference<K, V> {
    final String message;
    NullOutputExceptionReference(String message) {
      this.message = message;
    }
    public V get() {
      return null;
    }
    public ValueReference<K, V> copyFor(
        ReferenceEntry<K, V> entry) {
      return this;
    }
    public V waitForValue() {
      throw new NullOutputException(message);
    }
  }

  /** Used to provide computation exceptions to other threads. */
  private static class ComputationExceptionReference<K, V>
      implements ValueReference<K, V> {
    final Throwable t;
    ComputationExceptionReference(Throwable t) {
      this.t = t;
    }
    public V get() {
      return null;
    }
    public ValueReference<K, V> copyFor(
        ReferenceEntry<K, V> entry) {
      return this;
    }
    public V waitForValue() {
      throw new AsynchronousComputationException(t);
    }
  }

  /** Wrapper class ensures that queue isn't created until it's used. */
  private static class QueueHolder {
    static final FinalizableReferenceQueue queue
        = new FinalizableReferenceQueue();
  }

  /**
   * An entry in a reference map.
   */
  private interface ReferenceEntry<K, V> {
    /**
     * Gets the value reference from this entry.
     */
    ValueReference<K, V> getValueReference();

    /**
     * Sets the value reference for this entry.
     *
     * @param valueReference
     */
    void setValueReference(ValueReference<K, V> valueReference);

    /**
     * Removes this entry from the map if its value reference hasn't
     * changed.  Used to clean up after values. The value reference can
     * just call this method on the entry so it doesn't have to keep
     * its own reference to the map.
     */
    void valueReclaimed();

    /** Gets the next entry in the chain. */
    ReferenceEntry<K, V> getNext();

    /** Gets the entry's hash. */
    int getHash();

    /** Gets the key for this entry. */
    public K getKey();
  }

  /**
   * Used for strongly-referenced keys.
   */
  private static class StrongEntry<K, V> implements ReferenceEntry<K, V> {
    final K key;

    StrongEntry(Internals<K, V, ReferenceEntry<K, V>> internals, K key,
        int hash) {
      this.internals = internals;
      this.key = key;
      this.hash = hash;
    }

    public K getKey() {
      return this.key;
    }

    // The code below is exactly the same for each entry type.

    final Internals<K, V, ReferenceEntry<K, V>> internals;
    final int hash;
    volatile ValueReference<K, V> valueReference = computing();

    public ValueReference<K, V> getValueReference() {
      return valueReference;
    }
    public void setValueReference(
        ValueReference<K, V> valueReference) {
      this.valueReference = valueReference;
    }
    public void valueReclaimed() {
      internals.removeEntry(this, null);
    }
    public ReferenceEntry<K, V> getNext() {
      return null;
    }
    public int getHash() {
      return hash;
    }
  }

  private static class LinkedStrongEntry<K, V> extends StrongEntry<K, V> {

    LinkedStrongEntry(Internals<K, V, ReferenceEntry<K, V>> internals,
        K key, int hash, ReferenceEntry<K, V> next) {
      super(internals, key, hash);
      this.next = next;
    }

    final ReferenceEntry<K, V> next;

    @Override public ReferenceEntry<K, V> getNext() {
      return next;
    }
  }

  /**
   * Used for softly-referenced keys.
   */
  private static class SoftEntry<K, V> extends FinalizableSoftReference<K>
      implements ReferenceEntry<K, V> {
    SoftEntry(Internals<K, V, ReferenceEntry<K, V>> internals, K key,
        int hash) {
      super(key, QueueHolder.queue);
      this.internals = internals;
      this.hash = hash;
    }

    public K getKey() {
      return get();
    }

    public void finalizeReferent() {
      internals.removeEntry(this);
    }

    // The code below is exactly the same for each entry type.

    final Internals<K, V, ReferenceEntry<K, V>> internals;
    final int hash;
    volatile ValueReference<K, V> valueReference = computing();

    public ValueReference<K, V> getValueReference() {
      return valueReference;
    }
    public void setValueReference(
        ValueReference<K, V> valueReference) {
      this.valueReference = valueReference;
    }
    public void valueReclaimed() {
      internals.removeEntry(this, null);
    }
    public ReferenceEntry<K, V> getNext() {
      return null;
    }
    public int getHash() {
      return hash;
    }
  }

  private static class LinkedSoftEntry<K, V> extends SoftEntry<K, V> {
    LinkedSoftEntry(Internals<K, V, ReferenceEntry<K, V>> internals,
        K key, int hash, ReferenceEntry<K, V> next) {
      super(internals, key, hash);
      this.next = next;
    }

    final ReferenceEntry<K, V> next;

    @Override public ReferenceEntry<K, V> getNext() {
      return next;
    }
  }

  /**
   * Used for weakly-referenced keys.
   */
  private static class WeakEntry<K, V> extends FinalizableWeakReference<K>
      implements ReferenceEntry<K, V> {
    WeakEntry(Internals<K, V, ReferenceEntry<K, V>> internals, K key,
        int hash) {
      super(key, QueueHolder.queue);
      this.internals = internals;
      this.hash = hash;
    }

    public K getKey() {
      return get();
    }

    public void finalizeReferent() {
      internals.removeEntry(this);
    }

    // The code below is exactly the same for each entry type.

    final Internals<K, V, ReferenceEntry<K, V>> internals;
    final int hash;
    volatile ValueReference<K, V> valueReference = computing();

    public ValueReference<K, V> getValueReference() {
      return valueReference;
    }
    public void setValueReference(
        ValueReference<K, V> valueReference) {
      this.valueReference = valueReference;
    }
    public void valueReclaimed() {
      internals.removeEntry(this, null);
    }
    public ReferenceEntry<K, V> getNext() {
      return null;
    }
    public int getHash() {
      return hash;
    }
  }

  private static class LinkedWeakEntry<K, V> extends WeakEntry<K, V> {
    LinkedWeakEntry(Internals<K, V, ReferenceEntry<K, V>> internals,
        K key, int hash, ReferenceEntry<K, V> next) {
      super(internals, key, hash);
      this.next = next;
    }

    final ReferenceEntry<K, V> next;

    @Override public ReferenceEntry<K, V> getNext() {
      return next;
    }
  }

  /** References a weak value. */
  private static class WeakValueReference<K, V>
      extends FinalizableWeakReference<V>
      implements ValueReference<K, V> {
    final ReferenceEntry<K, V> entry;

    WeakValueReference(V referent, ReferenceEntry<K, V> entry) {
      super(referent, QueueHolder.queue);
      this.entry = entry;
    }

    public void finalizeReferent() {
      entry.valueReclaimed();
    }

    public ValueReference<K, V> copyFor(
        ReferenceEntry<K, V> entry) {
      return new WeakValueReference<K, V>(get(), entry);
    }

    public V waitForValue() {
      return get();
    }
  }

  /** References a soft value. */
  private static class SoftValueReference<K, V>
      extends FinalizableSoftReference<V>
      implements ValueReference<K, V> {
    final ReferenceEntry<K, V> entry;

    SoftValueReference(V referent, ReferenceEntry<K, V> entry) {
      super(referent, QueueHolder.queue);
      this.entry = entry;
    }

    public void finalizeReferent() {
      entry.valueReclaimed();
    }

    public ValueReference<K, V> copyFor(
        ReferenceEntry<K, V> entry) {
      return new SoftValueReference<K, V>(get(), entry);
    }

    public V waitForValue() {
      return get();
    }
  }

  /** References a strong value. */
  private static class StrongValueReference<K, V>
      implements ValueReference<K, V> {
    final V referent;

    StrongValueReference(V referent) {
      this.referent = referent;
    }

    public V get() {
      return referent;
    }

    public ValueReference<K, V> copyFor(
        ReferenceEntry<K, V> entry) {
      return this;
    }

    public V waitForValue() {
      return get();
    }
  }
}
