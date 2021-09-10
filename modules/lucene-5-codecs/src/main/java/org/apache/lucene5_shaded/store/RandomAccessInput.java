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
package org.apache.lucene5_shaded.store;


import java.io.IOException;

/** 
 * Random Access Index API.
 * Unlike {@link IndexInput}, this has no concept of file position, all reads
 * are absolute. However, like IndexInput, it is only intended for use by a single thread.
 */
public interface RandomAccessInput {
  
  /** 
   * Reads a byte at the given position in the file
   * @see DataInput#readByte
   */
  public byte readByte(long pos) throws IOException;
  /** 
   * Reads a short at the given position in the file
   * @see DataInput#readShort
   */
  public short readShort(long pos) throws IOException;
  /** 
   * Reads an integer at the given position in the file
   * @see DataInput#readInt
   */
  public int readInt(long pos) throws IOException;
  /** 
   * Reads a long at the given position in the file
   * @see DataInput#readLong
   */
  public long readLong(long pos) throws IOException;
}
