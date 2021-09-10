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


/**
 * A builder for {@link IntsRef} instances.
 * @lucene.internal
 */
public class IntsRefBuilder {

  private final IntsRef ref;

  /** Sole constructor. */
  public IntsRefBuilder() {
    ref = new IntsRef();
  }

  /** Return a reference to the ints of this builder. */
  public int[] ints() {
    return ref.ints;
  }

  /** Return the number of ints in this buffer. */
  public int length() {
    return ref.length;
  }

  /** Set the length. */
  public void setLength(int length) {
    this.ref.length = length;
  }

  /** Empty this builder. */
  public void clear() {
    setLength(0);
  }

  /** Return the int at the given offset. */
  public int intAt(int offset) {
    return ref.ints[offset];
  }

  /** Set an int. */
  public void setIntAt(int offset, int b) {
    ref.ints[offset] = b;
  }

  /** Append the provided int to this buffer. */
  public void append(int i) {
    grow(ref.length + 1);
    ref.ints[ref.length++] = i;
  }

  /**
   * Used to grow the reference array.
   *
   * In general this should not be used as it does not take the offset into account.
   * @lucene.internal */
  public void grow(int newLength) {
    ref.ints = ArrayUtil.grow(ref.ints, newLength);
  }

  /**
   * Copies the given array into this instance.
   */
  public void copyInts(int[] otherInts, int otherOffset, int otherLength) {
    grow(otherLength);
    System.arraycopy(otherInts, otherOffset, ref.ints, 0, otherLength);
    ref.length = otherLength;
  }

  /**
   * Copies the given array into this instance.
   */
  public void copyInts(IntsRef ints) {
    copyInts(ints.ints, ints.offset, ints.length);
  }

  /**
   * Copy the given UTF-8 bytes into this builder. Works as if the bytes were
   * first converted from UTF-8 to UTF-32 and then copied into this builder.
   */
  public void copyUTF8Bytes(BytesRef bytes) {
    grow(bytes.length);
    ref.length = UnicodeUtil.UTF8toUTF32(bytes, ref.ints);
  }

  /**
   * Return a {@link IntsRef} that points to the internal content of this
   * builder. Any update to the content of this builder might invalidate
   * the provided <code>ref</code> and vice-versa.
   */
  public IntsRef get() {
    assert ref.offset == 0 : "Modifying the offset of the returned ref is illegal";
    return ref;
  }

  /** Build a new {@link CharsRef} that has the same content as this builder. */
  public IntsRef toIntsRef() {
    return IntsRef.deepCopyOf(get());
  }

  @Override
  public boolean equals(Object obj) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }
}
