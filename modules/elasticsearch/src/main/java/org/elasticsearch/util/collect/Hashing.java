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

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * Static methods for implementing hash-based collections.
 *
 * @author Kevin Bourrillion
 * @author Jesse Wilson
 */
@GwtCompatible
final class Hashing {
  private Hashing() {}

  /*
   * This method was written by Doug Lea with assistance from members of JCP
   * JSR-166 Expert Group and released to the public domain, as explained at
   * http://creativecommons.org/licenses/publicdomain
   */
  static int smear(int hashCode) {
    hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
    return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
  }

  // We use power-of-2 tables, and this is the highest int that's a power of 2
  private static final int MAX_TABLE_SIZE = 1 << 30;

  // If the set has this many elements, it will "max out" the table size
  private static final int CUTOFF = 1 << 29;

  // Size the table to be at most 50% full, if possible
  static int chooseTableSize(int setSize) {
    if (setSize < CUTOFF) {
      return Integer.highestOneBit(setSize) << 2;
    }

    // The table can't be completely full or we'll get infinite reprobes
    checkArgument(setSize < MAX_TABLE_SIZE, "collection too large");
    return MAX_TABLE_SIZE;
  }
}
