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

/**
 * A native int hash-based set where one value is reserved to mean "EMPTY" internally. The space overhead is fairly low
 * as there is only one power-of-two sized int[] to hold the values.  The set is re-hashed when adding a value that
 * would make it &gt;= 75% full.  Consider extending and over-riding {@link #hash(int)} if the values might be poor
 * hash keys; Lucene docids should be fine.
 * The internal fields are exposed publicly to enable more efficient use at the expense of better O-O principles.
 * <p>
 * To iterate over the integers held in this set, simply use code like this:
 * <pre class="prettyprint">
 * SentinelIntSet set = ...
 * for (int v : set.keys) {
 *   if (v == set.emptyVal)
 *     continue;
 *   //use v...
 * }</pre>
 *
 * @lucene.internal
 */
public class SentinelIntSet {
  /** A power-of-2 over-sized array holding the integers in the set along with empty values. */
  public int[] keys;
  public int count;
  public final int emptyVal;
  /** the count at which a rehash should be done */
  public int rehashCount;

  /**
   *
   * @param size  The minimum number of elements this set should be able to hold without rehashing
   *              (i.e. the slots are guaranteed not to change)
   * @param emptyVal The integer value to use for EMPTY
   */
  public SentinelIntSet(int size, int emptyVal) {
    this.emptyVal = emptyVal;
    int tsize = Math.max(BitUtil.nextHighestPowerOfTwo(size), 1);
    rehashCount = tsize - (tsize>>2);
    if (size >= rehashCount) {  // should be able to hold "size" w/o re-hashing
      tsize <<= 1;
      rehashCount = tsize - (tsize>>2);
    }
    keys = new int[tsize];
    if (emptyVal != 0)
      clear();
  }

  public void clear() {
    Arrays.fill(keys, emptyVal);
    count = 0;
  }

  /** (internal) Return the hash for the key. The default implementation just returns the key,
   * which is not appropriate for general purpose use.
   */
  public int hash(int key) {
    return key;
  }

  /** The number of integers in this set. */
  public int size() { return count; }

  /** (internal) Returns the slot for this key */
  public int getSlot(int key) {
    assert key != emptyVal;
    int h = hash(key);
    int s = h & (keys.length-1);
    if (keys[s] == key || keys[s]== emptyVal) return s;

    int increment = (h>>7)|1;
    do {
      s = (s + increment) & (keys.length-1);
    } while (keys[s] != key && keys[s] != emptyVal);
    return s;
  }

  /** (internal) Returns the slot for this key, or -slot-1 if not found */
  public int find(int key) {
    assert key != emptyVal;
    int h = hash(key);
    int s = h & (keys.length-1);
    if (keys[s] == key) return s;
    if (keys[s] == emptyVal) return -s-1;

    int increment = (h>>7)|1;
    for(;;) {
      s = (s + increment) & (keys.length-1);
      if (keys[s] == key) return s;
      if (keys[s] == emptyVal) return -s-1;
    }
  }

  /** Does this set contain the specified integer? */
  public boolean exists(int key) {
    return find(key) >= 0;
  }

  /** Puts this integer (key) in the set, and returns the slot index it was added to.
   * It rehashes if adding it would make the set more than 75% full. */
  public int put(int key) {
    int s = find(key);
    if (s < 0) {
      count++;
      if (count >= rehashCount) {
        rehash();
        s = getSlot(key);
      } else {
        s = -s-1;
      }
      keys[s] = key;
    }
    return s;
  }

  /** (internal) Rehashes by doubling {@code int[] key} and filling with the old values. */
  public void rehash() {
    int newSize = keys.length << 1;
    int[] oldKeys = keys;
    keys = new int[newSize];
    if (emptyVal != 0) Arrays.fill(keys, emptyVal);

    for (int key : oldKeys) {
      if (key == emptyVal) continue;
      int newSlot = getSlot(key);
      keys[newSlot] = key;
    }
    rehashCount = newSize - (newSize>>2);
  }

  /** Return the memory footprint of this class in bytes. */
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
          RamUsageEstimator.NUM_BYTES_INT * 3
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF)
        + RamUsageEstimator.sizeOf(keys);
  }
}
