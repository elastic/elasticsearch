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
package org.apache.lucene5_shaded.codecs.compressing;


import java.io.IOException;

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.util.BytesRef;

/**
 * A decompressor.
 */
public abstract class Decompressor implements Cloneable {

  /** Sole constructor, typically called from sub-classes. */
  protected Decompressor() {}

  /**
   * Decompress bytes that were stored between offsets <code>offset</code> and
   * <code>offset+length</code> in the original stream from the compressed
   * stream <code>in</code> to <code>bytes</code>. After returning, the length
   * of <code>bytes</code> (<code>bytes.length</code>) must be equal to
   * <code>length</code>. Implementations of this method are free to resize
   * <code>bytes</code> depending on their needs.
   *
   * @param in the input that stores the compressed stream
   * @param originalLength the length of the original data (before compression)
   * @param offset bytes before this offset do not need to be decompressed
   * @param length bytes after <code>offset+length</code> do not need to be decompressed
   * @param bytes a {@link BytesRef} where to store the decompressed data
   */
  public abstract void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException;

  @Override
  public abstract Decompressor clone();

}
