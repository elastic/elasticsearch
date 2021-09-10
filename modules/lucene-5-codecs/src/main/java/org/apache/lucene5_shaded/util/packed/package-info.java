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

/** 
 * Packed integer arrays and streams.
 * 
 * <p>
 *     The packed package provides
 *     <ul>
 *      <li>sequential and random access capable arrays of positive longs,</li>
 *      <li>routines for efficient serialization and deserialization of streams of packed integers.</li>
 *     </ul>
 * 
 *     The implementations provide different trade-offs between memory usage and
 *     access speed. The standard usage scenario is replacing large int or long
 *     arrays in order to reduce the memory footprint.
 * <p>
 *     The main access point is the {@link org.apache.lucene5_shaded.util.packed.PackedInts} factory.
 * 
 * <h3>In-memory structures</h3>
 * 
 * <ul>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.PackedInts.Mutable}</b><ul>
 *         <li>Only supports positive longs.</li>
 *         <li>Requires the number of bits per value to be known in advance.</li>
 *         <li>Random-access for both writing and reading.</li>
 *     </ul></li>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.GrowableWriter}</b><ul>
 *         <li>Same as PackedInts.Mutable but grows the number of bits per values when needed.</li>
 *         <li>Useful to build a PackedInts.Mutable from a read-once stream of longs.</li>
 *     </ul></li>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.PagedGrowableWriter}</b><ul>
 *         <li>Slices data into fixed-size blocks stored in GrowableWriters.</li>
 *         <li>Supports more than 2B values.</li>
 *         <li>You should use PackedLongValues instead if you don't need random write access.</li>
 *     </ul></li>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.PackedLongValues#deltaPackedBuilder}</b><ul>
 *         <li>Can store any sequence of longs.</li>
 *         <li>Compression is good when values are close to each other.</li>
 *         <li>Supports random reads, but only sequential writes.</li>
 *         <li>Can address up to 2^42 values.</li>
 *     </ul></li>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.PackedLongValues#packedBuilder}</b><ul>
 *         <li>Same as deltaPackedBuilder but assumes values are 0-based.</li>
 *     </ul></li>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.PackedLongValues#monotonicBuilder}</b><ul>
 *         <li>Same as deltaPackedBuilder except that compression is good when the stream is a succession of affine functions.</li>
 *     </ul></li>
 * </ul>
 * 
 * <h3>Disk-based structures</h3>
 * 
 * <ul>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.PackedInts.Writer}, {@link org.apache.lucene5_shaded.util.packed.PackedInts.Reader}, {@link org.apache.lucene5_shaded.util.packed.PackedInts.ReaderIterator}</b><ul>
 *         <li>Only supports positive longs.</li>
 *         <li>Requires the number of bits per value to be known in advance.</li>
 *         <li>Supports both fast sequential access with low memory footprint with ReaderIterator and random-access by either loading values in memory or leaving them on disk with Reader.</li>
 *     </ul></li>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.BlockPackedWriter}, {@link org.apache.lucene5_shaded.util.packed.BlockPackedReader}, {@link org.apache.lucene5_shaded.util.packed.BlockPackedReaderIterator}</b><ul>
 *         <li>Splits the stream into fixed-size blocks.</li>
 *         <li>Compression is good when values are close to each other.</li>
 *         <li>Can address up to 2B * blockSize values.</li>
 *     </ul></li>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.MonotonicBlockPackedWriter}, {@link org.apache.lucene5_shaded.util.packed.MonotonicBlockPackedReader}</b><ul>
 *         <li>Same as the non-monotonic variants except that compression is good when the stream is a succession of affine functions.</li>
 *         <li>The reason why there is no sequential access is that if you need sequential access, you should rather delta-encode and use BlockPackedWriter.</li>
 *     </ul></li>
 *     <li><b>{@link org.apache.lucene5_shaded.util.packed.PackedDataOutput}, {@link org.apache.lucene5_shaded.util.packed.PackedDataInput}</b><ul>
 *         <li>Writes sequences of longs where each long can use any number of bits.</li>
 *     </ul></li>
 * </ul>
 */
package org.apache.lucene5_shaded.util.packed;
