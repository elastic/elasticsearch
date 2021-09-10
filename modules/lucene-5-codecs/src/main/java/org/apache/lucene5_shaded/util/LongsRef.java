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


/** Represents long[], as a slice (offset + length) into an
 *  existing long[].  The {@link #longs} member should never be null; use
 *  {@link #EMPTY_LONGS} if necessary.
 *
 *  @lucene.internal */
public final class LongsRef implements Comparable<LongsRef>, Cloneable {
  /** An empty long array for convenience */
  public static final long[] EMPTY_LONGS = new long[0];

  /** The contents of the LongsRef. Should never be {@code null}. */
  public long[] longs;
  /** Offset of first valid long. */
  public int offset;
  /** Length of used longs. */
  public int length;

  /** Create a LongsRef with {@link #EMPTY_LONGS} */
  public LongsRef() {
    longs = EMPTY_LONGS;
  }

  /** 
   * Create a LongsRef pointing to a new array of size <code>capacity</code>.
   * Offset and length will both be zero.
   */
  public LongsRef(int capacity) {
    longs = new long[capacity];
  }

  /** This instance will directly reference longs w/o making a copy.
   * longs should not be null */
  public LongsRef(long[] longs, int offset, int length) {
    this.longs = longs;
    this.offset = offset;
    this.length = length;
    assert isValid();
  }

  /**
   * Returns a shallow clone of this instance (the underlying longs are
   * <b>not</b> copied and will be shared by both the returned object and this
   * object.
   * 
   * @see #deepCopyOf
   */  
  @Override
  public LongsRef clone() {
    return new LongsRef(longs, offset, length);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 0;
    final long end = offset + length;
    for(int i = offset; i < end; i++) {
      result = prime * result + (int) (longs[i] ^ (longs[i]>>>32));
    }
    return result;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other instanceof LongsRef) {
      return this.longsEquals((LongsRef) other);
    }
    return false;
  }

  public boolean longsEquals(LongsRef other) {
    if (length == other.length) {
      int otherUpto = other.offset;
      final long[] otherInts = other.longs;
      final long end = offset + length;
      for(int upto=offset; upto<end; upto++,otherUpto++) {
        if (longs[upto] != otherInts[otherUpto]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  /** Signed int order comparison */
  @Override
  public int compareTo(LongsRef other) {
    if (this == other) return 0;

    final long[] aInts = this.longs;
    int aUpto = this.offset;
    final long[] bInts = other.longs;
    int bUpto = other.offset;

    final long aStop = aUpto + Math.min(this.length, other.length);

    while(aUpto < aStop) {
      long aInt = aInts[aUpto++];
      long bInt = bInts[bUpto++];
      if (aInt > bInt) {
        return 1;
      } else if (aInt < bInt) {
        return -1;
      }
    }

    // One is a prefix of the other, or, they are equal:
    return this.length - other.length;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    final long end = offset + length;
    for(int i=offset;i<end;i++) {
      if (i > offset) {
        sb.append(' ');
      }
      sb.append(Long.toHexString(longs[i]));
    }
    sb.append(']');
    return sb.toString();
  }
  
  /**
   * Creates a new LongsRef that points to a copy of the longs from 
   * <code>other</code>
   * <p>
   * The returned IntsRef will have a length of other.length
   * and an offset of zero.
   */
  public static LongsRef deepCopyOf(LongsRef other) {
    return new LongsRef(Arrays.copyOfRange(other.longs, other.offset, other.offset + other.length), 0, other.length);
  }
  
  /** 
   * Performs internal consistency checks.
   * Always returns true (or throws IllegalStateException) 
   */
  public boolean isValid() {
    if (longs == null) {
      throw new IllegalStateException("longs is null");
    }
    if (length < 0) {
      throw new IllegalStateException("length is negative: " + length);
    }
    if (length > longs.length) {
      throw new IllegalStateException("length is out of bounds: " + length + ",longs.length=" + longs.length);
    }
    if (offset < 0) {
      throw new IllegalStateException("offset is negative: " + offset);
    }
    if (offset > longs.length) {
      throw new IllegalStateException("offset out of bounds: " + offset + ",longs.length=" + longs.length);
    }
    if (offset + length < 0) {
      throw new IllegalStateException("offset+length is negative: offset=" + offset + ",length=" + length);
    }
    if (offset + length > longs.length) {
      throw new IllegalStateException("offset+length out of bounds: offset=" + offset + ",length=" + length + ",longs.length=" + longs.length);
    }
    return true;
  }
}
