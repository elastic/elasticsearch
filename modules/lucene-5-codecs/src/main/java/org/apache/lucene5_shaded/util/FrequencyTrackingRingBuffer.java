/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.util;


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A ring buffer that tracks the frequency of the integers that it contains.
 * This is typically useful to track the hash codes of popular recently-used
 * items.
 *
 * This data-structure requires 22 bytes per entry on average (between 16 and
 * 28).
 *
 * @lucene.internal
 */
public final class FrequencyTrackingRingBuffer implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FrequencyTrackingRingBuffer.class);

  private final int maxSize;
  private final int[] buffer;
  private int position;
  private final IntBag frequencies;

  /** Create a new ring buffer that will contain at most <code>maxSize</code> items.
   *  This buffer will initially contain <code>maxSize</code> times the
   *  <code>sentinel</code> value. */
  public FrequencyTrackingRingBuffer(int maxSize, int sentinel) {
    if (maxSize < 2) {
      throw new IllegalArgumentException("maxSize must be at least 2");
    }
    this.maxSize = maxSize;
    buffer = new int[maxSize];
    position = 0;
    frequencies = new IntBag(maxSize);

    Arrays.fill(buffer, sentinel);
    for (int i = 0; i < maxSize; ++i) {
      frequencies.add(sentinel);
    }
    assert frequencies.frequency(sentinel) == maxSize;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED
        + frequencies.ramBytesUsed()
        + RamUsageEstimator.sizeOf(buffer);
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  /**
   * Add a new item to this ring buffer, potentially removing the oldest
   * entry from this buffer if it is already full.
   */
  public void add(int i) {
    // remove the previous value
    final int removed = buffer[position];
    final boolean removedFromBag = frequencies.remove(removed);
    assert removedFromBag;
    // add the new value
    buffer[position] = i;
    frequencies.add(i);
    // increment the position
    position += 1;
    if (position == maxSize) {
      position = 0;
    }
  }

  /**
   * Returns the frequency of the provided key in the ring buffer.
   */
  public int frequency(int key) {
    return frequencies.frequency(key);
  }

  // pkg-private for testing
  Map<Integer, Integer> asFrequencyMap() {
    return frequencies.asMap();
  }

  /**
   * A bag of integers.
   * Since in the context of the ring buffer the maximum size is known up-front
   * there is no need to worry about resizing the underlying storage.
   */
  private static class IntBag implements Accountable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IntBag.class);

    private final int[] keys;
    private final int[] freqs;
    private final int mask;

    IntBag(int maxSize) {
      // load factor of 2/3
      int capacity = Math.max(2, maxSize * 3 / 2);
      // round up to the next power of two
      capacity = Integer.highestOneBit(capacity - 1) << 1;
      assert capacity > maxSize;
      keys = new int[capacity];
      freqs = new int[capacity];
      mask = capacity - 1;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(keys)
          + RamUsageEstimator.sizeOf(freqs);
    }

    @Override
    public Collection<Accountable> getChildResources() {
      return Collections.emptyList();
    }

    /** Return the frequency of the give key in the bag. */
    int frequency(int key) {
      for (int slot = key & mask; ; slot = (slot + 1) & mask) {
        if (keys[slot] == key) {
          return freqs[slot];
        } else if (freqs[slot] == 0) {
          return 0;
        }
      }
    }

    /** Increment the frequency of the given key by 1 and return its new frequency. */
    int add(int key) {
      for (int slot = key & mask; ; slot = (slot + 1) & mask) {
        if (freqs[slot] == 0) {
          keys[slot] = key;
          return freqs[slot] = 1;
        } else if (keys[slot] == key) {
          return ++freqs[slot];
        }
      }
    }

    /** Decrement the frequency of the given key by one, or do nothing if the
     *  key is not present in the bag. Returns true iff the key was contained
     *  in the bag. */
    boolean remove(int key) {
      for (int slot = key & mask; ; slot = (slot + 1) & mask) {
        if (freqs[slot] == 0) {
          // no such key in the bag
          return false;
        } else if (keys[slot] == key) {
          final int newFreq = --freqs[slot];
          if (newFreq == 0) { // removed
            relocateAdjacentKeys(slot);
          }
          return true;
        }
      }
    }

    private void relocateAdjacentKeys(int freeSlot) {
      for (int slot = (freeSlot + 1) & mask; ; slot = (slot + 1) & mask) {
        final int freq = freqs[slot];
        if (freq == 0) {
          // end of the collision chain, we're done
          break;
        }
        final int key = keys[slot];
        // the slot where <code>key</code> should be if there were no collisions
        final int expectedSlot = key & mask;
        // if the free slot is between the expected slot and the slot where the
        // key is, then we can relocate there
        if (between(expectedSlot, slot, freeSlot)) {
          keys[freeSlot] = key;
          freqs[freeSlot] = freq;
          // slot is the new free slot
          freqs[slot] = 0;
          freeSlot = slot;
        }
      }
    }

    /** Given a chain of occupied slots between <code>chainStart</code>
     *  and <code>chainEnd</code>, return whether <code>slot</code> is
     *  between the start and end of the chain. */
    private static boolean between(int chainStart, int chainEnd, int slot) {
      if (chainStart <= chainEnd) {
        return chainStart <= slot && slot <= chainEnd;
      } else {
        // the chain is across the end of the array
        return slot >= chainStart || slot <= chainEnd;
      }
    }

    Map<Integer, Integer> asMap() {
      Map<Integer, Integer> map = new HashMap<>();
      for (int i = 0; i < keys.length; ++i) {
        if (freqs[i] > 0) {
          map.put(keys[i], freqs[i]);
        }
      }
      return map;
    }

  }

}
