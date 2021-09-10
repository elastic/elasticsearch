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
package org.apache.lucene5_shaded.codecs.lucene54;


import java.io.IOException;

import org.apache.lucene5_shaded.codecs.DocValuesConsumer;
import org.apache.lucene5_shaded.codecs.DocValuesFormat;
import org.apache.lucene5_shaded.codecs.DocValuesProducer;
import org.apache.lucene5_shaded.index.DocValuesType;
import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.index.SegmentWriteState;
import org.apache.lucene5_shaded.util.SmallFloat;
import org.apache.lucene5_shaded.util.packed.DirectWriter;

/**
 * Lucene 5.4 DocValues format.
 * <p>
 * Encodes the five per-document value types (Numeric,Binary,Sorted,SortedSet,SortedNumeric) with these strategies:
 * <p>
 * {@link DocValuesType#NUMERIC NUMERIC}:
 * <ul>
 *    <li>Delta-compressed: per-document integers written as deltas from the minimum value,
 *        compressed with bitpacking. For more information, see {@link DirectWriter}.
 *    <li>Table-compressed: when the number of unique values is very small (&lt; 256), and
 *        when there are unused "gaps" in the range of values used (such as {@link SmallFloat}), 
 *        a lookup table is written instead. Each per-document entry is instead the ordinal 
 *        to this table, and those ordinals are compressed with bitpacking ({@link DirectWriter}). 
 *    <li>GCD-compressed: when all numbers share a common divisor, such as dates, the greatest
 *        common denominator (GCD) is computed, and quotients are stored using Delta-compressed Numerics.
 *    <li>Monotonic-compressed: when all numbers are monotonically increasing offsets, they are written
 *        as blocks of bitpacked integers, encoding the deviation from the expected delta.
 *    <li>Const-compressed: when there is only one possible non-missing value, only the missing
 *        bitset is encoded.
 *    <li>Sparse-compressed: only documents with a value are stored, and lookups are performed
 *        using binary search.
 * </ul>
 * <p>
 * {@link DocValuesType#BINARY BINARY}:
 * <ul>
 *    <li>Fixed-width Binary: one large concatenated byte[] is written, along with the fixed length.
 *        Each document's value can be addressed directly with multiplication ({@code docID * length}). 
 *    <li>Variable-width Binary: one large concatenated byte[] is written, along with end addresses 
 *        for each document. The addresses are written as Monotonic-compressed numerics.
 *    <li>Prefix-compressed Binary: values are written in chunks of 16, with the first value written
 *        completely and other values sharing prefixes. chunk addresses are written as Monotonic-compressed
 *        numerics. A reverse lookup index is written from a portion of every 1024th term.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED SORTED}:
 * <ul>
 *    <li>Sorted: a mapping of ordinals to deduplicated terms is written as Binary, 
 *        along with the per-document ordinals written using one of the numeric strategies above.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED_SET SORTED_SET}:
 * <ul>
 *    <li>Single: if all documents have 0 or 1 value, then data are written like SORTED.
 *    <li>SortedSet table: when there are few unique sets of values (&lt; 256) then each set is assigned
 *        an id, a lookup table is written and the mapping from document to set id is written using the
 *        numeric strategies above.
 *    <li>SortedSet: a mapping of ordinals to deduplicated terms is written as Binary, 
 *        an ordinal list and per-document index into this list are written using the numeric strategies 
 *        above.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED_NUMERIC SORTED_NUMERIC}:
 * <ul>
 *    <li>Single: if all documents have 0 or 1 value, then data are written like NUMERIC.
 *    <li>SortedSet table: when there are few unique sets of values (&lt; 256) then each set is assigned
 *        an id, a lookup table is written and the mapping from document to set id is written using the
 *        numeric strategies above.
 *    <li>SortedNumeric: a value list and per-document index into this list are written using the numeric
 *        strategies above.
 * </ul>
 * <p>
 * Files:
 * <ol>
 *   <li><tt>.dvd</tt>: DocValues data</li>
 *   <li><tt>.dvm</tt>: DocValues metadata</li>
 * </ol>
 * @lucene.experimental
 */
public final class Lucene54DocValuesFormat extends DocValuesFormat {

  /** Sole Constructor */
  public Lucene54DocValuesFormat() {
    super("Lucene54");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene54DocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Lucene54DocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }
  
  static final String DATA_CODEC = "Lucene54DocValuesData";
  static final String DATA_EXTENSION = "dvd";
  static final String META_CODEC = "Lucene54DocValuesMetadata";
  static final String META_EXTENSION = "dvm";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  
  // indicates docvalues type
  static final byte NUMERIC = 0;
  static final byte BINARY = 1;
  static final byte SORTED = 2;
  static final byte SORTED_SET = 3;
  static final byte SORTED_NUMERIC = 4;
  
  // address terms in blocks of 16 terms
  static final int INTERVAL_SHIFT = 4;
  static final int INTERVAL_COUNT = 1 << INTERVAL_SHIFT;
  static final int INTERVAL_MASK = INTERVAL_COUNT - 1;
  
  // build reverse index from every 1024th term
  static final int REVERSE_INTERVAL_SHIFT = 10;
  static final int REVERSE_INTERVAL_COUNT = 1 << REVERSE_INTERVAL_SHIFT;
  static final int REVERSE_INTERVAL_MASK = REVERSE_INTERVAL_COUNT - 1;
  
  // for conversion from reverse index to block
  static final int BLOCK_INTERVAL_SHIFT = REVERSE_INTERVAL_SHIFT - INTERVAL_SHIFT;
  static final int BLOCK_INTERVAL_COUNT = 1 << BLOCK_INTERVAL_SHIFT;
  static final int BLOCK_INTERVAL_MASK = BLOCK_INTERVAL_COUNT - 1;

  /** Compressed using packed blocks of ints. */
  static final int DELTA_COMPRESSED = 0;
  /** Compressed by computing the GCD. */
  static final int GCD_COMPRESSED = 1;
  /** Compressed by giving IDs to unique values. */
  static final int TABLE_COMPRESSED = 2;
  /** Compressed with monotonically increasing values */
  static final int MONOTONIC_COMPRESSED = 3;
  /** Compressed with constant value (uses only missing bitset) */
  static final int CONST_COMPRESSED = 4;
  /** Compressed with sparse arrays. */
  static final int SPARSE_COMPRESSED = 5;

  /** Uncompressed binary, written directly (fixed length). */
  static final int BINARY_FIXED_UNCOMPRESSED = 0;
  /** Uncompressed binary, written directly (variable length). */
  static final int BINARY_VARIABLE_UNCOMPRESSED = 1;
  /** Compressed binary with shared prefixes */
  static final int BINARY_PREFIX_COMPRESSED = 2;

  /** Standard storage for sorted set values with 1 level of indirection:
   *  {@code docId -> address -> ord}. */
  static final int SORTED_WITH_ADDRESSES = 0;
  /** Single-valued sorted set values, encoded as sorted values, so no level
   *  of indirection: {@code docId -> ord}. */
  static final int SORTED_SINGLE_VALUED = 1;
  /** Compressed giving IDs to unique sets of values:
   * {@code docId -> setId -> ords} */
  static final int SORTED_SET_TABLE = 2;
  
  /** placeholder for missing offset that means there are no missing values */
  static final int ALL_LIVE = -1;
  /** placeholder for missing offset that means all values are missing */
  static final int ALL_MISSING = -2;
  
  // addressing uses 16k blocks
  static final int MONOTONIC_BLOCK_SIZE = 16384;
  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
}
