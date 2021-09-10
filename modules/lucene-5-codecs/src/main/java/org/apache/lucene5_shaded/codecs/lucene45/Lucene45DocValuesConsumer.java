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
package org.apache.lucene5_shaded.codecs.lucene45;

import java.io.Closeable; // javadocs
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.DocValuesConsumer;
import org.apache.lucene5_shaded.index.DocValuesType;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.SegmentWriteState;
import org.apache.lucene5_shaded.store.IndexOutput;
import org.apache.lucene5_shaded.store.RAMOutputStream;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.MathUtil;
import org.apache.lucene5_shaded.util.StringHelper;
import org.apache.lucene5_shaded.util.packed.BlockPackedWriter;
import org.apache.lucene5_shaded.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene5_shaded.util.packed.PackedInts;

/** 
 * writer for 4.5 docvalues format
 * @deprecated only for old 4.x segments
 */
@Deprecated
class Lucene45DocValuesConsumer extends DocValuesConsumer implements Closeable {

  static final int BLOCK_SIZE = 16384;
  static final int ADDRESS_INTERVAL = 16;

  /** Compressed using packed blocks of ints. */
  public static final int DELTA_COMPRESSED = 0;
  /** Compressed by computing the GCD. */
  public static final int GCD_COMPRESSED = 1;
  /** Compressed by giving IDs to unique values. */
  public static final int TABLE_COMPRESSED = 2;
  
  /** Uncompressed binary, written directly (fixed length). */
  public static final int BINARY_FIXED_UNCOMPRESSED = 0;
  /** Uncompressed binary, written directly (variable length). */
  public static final int BINARY_VARIABLE_UNCOMPRESSED = 1;
  /** Compressed binary with shared prefixes */
  public static final int BINARY_PREFIX_COMPRESSED = 2;

  /** Standard storage for sorted set values with 1 level of indirection:
   *  docId -> address -> ord. */
  public static final int SORTED_SET_WITH_ADDRESSES = 0;
  /** Single-valued sorted set values, encoded as sorted values, so no level
   *  of indirection: docId -> ord. */
  public static final int SORTED_SET_SINGLE_VALUED_SORTED = 1;

  IndexOutput data, meta;
  final int maxDoc;
  
  /** expert: Creates a new writer */
  public Lucene45DocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    boolean success = false;
    try {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeHeader(data, dataCodec, Lucene45DocValuesFormat.VERSION_CURRENT);
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeHeader(meta, metaCodec, Lucene45DocValuesFormat.VERSION_CURRENT);
      maxDoc = state.segmentInfo.maxDoc();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }
  
  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    checkCanWrite(field);
    addNumericField(field, values, true);
  }

  void addNumericField(FieldInfo field, Iterable<Number> values, boolean optimizeStorage) throws IOException {
    long count = 0;
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    long gcd = 0;
    boolean missing = false;
    // TODO: more efficient?
    HashSet<Long> uniqueValues = null;
    if (optimizeStorage) {
      uniqueValues = new HashSet<>();

      for (Number nv : values) {
        final long v;
        if (nv == null) {
          v = 0;
          missing = true;
        } else {
          v = nv.longValue();
        }

        if (gcd != 1) {
          if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
            // in that case v - minValue might overflow and make the GCD computation return
            // wrong results. Since these extreme values are unlikely, we just discard
            // GCD computation for them
            gcd = 1;
          } else if (count != 0) { // minValue needs to be set first
            gcd = MathUtil.gcd(gcd, v - minValue);
          }
        }

        minValue = Math.min(minValue, v);
        maxValue = Math.max(maxValue, v);

        if (uniqueValues != null) {
          if (uniqueValues.add(v)) {
            if (uniqueValues.size() > 256) {
              uniqueValues = null;
            }
          }
        }

        ++count;
      }
    } else {
      for (@SuppressWarnings("unused") Number nv : values) {
        ++count;
      }
    }
    
    final long delta = maxValue - minValue;

    final int format;
    if (uniqueValues != null
        && (PackedInts.bitsRequired(uniqueValues.size() - 1) < PackedInts.unsignedBitsRequired(delta))
        && count <= Integer.MAX_VALUE) {
      format = TABLE_COMPRESSED;
    } else if (gcd != 0 && gcd != 1) {
      format = GCD_COMPRESSED;
    } else {
      format = DELTA_COMPRESSED;
    }
    meta.writeVInt(field.number);
    meta.writeByte(Lucene45DocValuesFormat.NUMERIC);
    meta.writeVInt(format);
    if (missing) {
      meta.writeLong(data.getFilePointer());
      writeMissingBitset(values);
    } else {
      meta.writeLong(-1L);
    }
    meta.writeVInt(PackedInts.VERSION_CURRENT);
    meta.writeLong(data.getFilePointer());
    meta.writeVLong(count);
    meta.writeVInt(BLOCK_SIZE);

    switch (format) {
      case GCD_COMPRESSED:
        meta.writeLong(minValue);
        meta.writeLong(gcd);
        final BlockPackedWriter quotientWriter = new BlockPackedWriter(data, BLOCK_SIZE);
        for (Number nv : values) {
          long value = nv == null ? 0 : nv.longValue();
          quotientWriter.add((value - minValue) / gcd);
        }
        quotientWriter.finish();
        break;
      case DELTA_COMPRESSED:
        final BlockPackedWriter writer = new BlockPackedWriter(data, BLOCK_SIZE);
        for (Number nv : values) {
          writer.add(nv == null ? 0 : nv.longValue());
        }
        writer.finish();
        break;
      case TABLE_COMPRESSED:
        final Long[] decode = uniqueValues.toArray(new Long[uniqueValues.size()]);
        final HashMap<Long,Integer> encode = new HashMap<>();
        meta.writeVInt(decode.length);
        for (int i = 0; i < decode.length; i++) {
          meta.writeLong(decode[i]);
          encode.put(decode[i], i);
        }
        final int bitsRequired = PackedInts.bitsRequired(uniqueValues.size() - 1);
        final PackedInts.Writer ordsWriter = PackedInts.getWriterNoHeader(data, PackedInts.Format.PACKED, (int) count, bitsRequired, PackedInts.DEFAULT_BUFFER_SIZE);
        for (Number nv : values) {
          ordsWriter.add(encode.get(nv == null ? 0 : nv.longValue()));
        }
        ordsWriter.finish();
        break;
      default:
        throw new AssertionError();
    }
  }
  
  // TODO: in some cases representing missing with minValue-1 wouldn't take up additional space and so on,
  // but this is very simple, and algorithms only check this for values of 0 anyway (doesnt slow down normal decode)
  void writeMissingBitset(Iterable<?> values) throws IOException {
    byte bits = 0;
    int count = 0;
    for (Object v : values) {
      if (count == 8) {
        data.writeByte(bits);
        count = 0;
        bits = 0;
      }
      if (v != null) {
        bits |= 1 << (count & 7);
      }
      count++;
    }
    if (count > 0) {
      data.writeByte(bits);
    }
  }

  @Override
  public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
    checkCanWrite(field);
    // write the byte[] data
    meta.writeVInt(field.number);
    meta.writeByte(Lucene45DocValuesFormat.BINARY);
    int minLength = Integer.MAX_VALUE;
    int maxLength = Integer.MIN_VALUE;
    final long startFP = data.getFilePointer();
    long count = 0;
    boolean missing = false;
    for(BytesRef v : values) {
      final int length;
      if (v == null) {
        length = 0;
        missing = true;
      } else {
        length = v.length;
      }
      minLength = Math.min(minLength, length);
      maxLength = Math.max(maxLength, length);
      if (v != null) {
        data.writeBytes(v.bytes, v.offset, v.length);
      }
      count++;
    }
    meta.writeVInt(minLength == maxLength ? BINARY_FIXED_UNCOMPRESSED : BINARY_VARIABLE_UNCOMPRESSED);
    if (missing) {
      meta.writeLong(data.getFilePointer());
      writeMissingBitset(values);
    } else {
      meta.writeLong(-1L);
    }
    meta.writeVInt(minLength);
    meta.writeVInt(maxLength);
    meta.writeVLong(count);
    meta.writeLong(startFP);
    
    // if minLength == maxLength, its a fixed-length byte[], we are done (the addresses are implicit)
    // otherwise, we need to record the length fields...
    if (minLength != maxLength) {
      meta.writeLong(data.getFilePointer());
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeVInt(BLOCK_SIZE);

      final MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(data, BLOCK_SIZE);
      long addr = 0;
      for (BytesRef v : values) {
        if (v != null) {
          addr += v.length;
        }
        writer.add(addr);
      }
      writer.finish();
    }
  }
  
  /** expert: writes a value dictionary for a sorted/sortedset field */
  protected void addTermsDict(FieldInfo field, final Iterable<BytesRef> values) throws IOException {
    // first check if its a "fixed-length" terms dict
    int minLength = Integer.MAX_VALUE;
    int maxLength = Integer.MIN_VALUE;
    for (BytesRef v : values) {
      minLength = Math.min(minLength, v.length);
      maxLength = Math.max(maxLength, v.length);
    }
    if (minLength == maxLength) {
      // no index needed: direct addressing by mult
      addBinaryField(field, values);
    } else {
      // header
      meta.writeVInt(field.number);
      meta.writeByte(Lucene45DocValuesFormat.BINARY);
      meta.writeVInt(BINARY_PREFIX_COMPRESSED);
      meta.writeLong(-1L);
      // now write the bytes: sharing prefixes within a block
      final long startFP = data.getFilePointer();
      // currently, we have to store the delta from expected for every 1/nth term
      // we could avoid this, but its not much and less overall RAM than the previous approach!
      RAMOutputStream addressBuffer = new RAMOutputStream();
      MonotonicBlockPackedWriter termAddresses = new MonotonicBlockPackedWriter(addressBuffer, BLOCK_SIZE);
      BytesRefBuilder lastTerm = new BytesRefBuilder();
      long count = 0;
      for (BytesRef v : values) {
        if (count % ADDRESS_INTERVAL == 0) {
          termAddresses.add(data.getFilePointer() - startFP);
          // force the first term in a block to be abs-encoded
          lastTerm.clear();
        }
        
        // prefix-code
        int sharedPrefix = StringHelper.bytesDifference(lastTerm.get(), v);
        data.writeVInt(sharedPrefix);
        data.writeVInt(v.length - sharedPrefix);
        data.writeBytes(v.bytes, v.offset + sharedPrefix, v.length - sharedPrefix);
        lastTerm.copyBytes(v);
        count++;
      }
      final long indexStartFP = data.getFilePointer();
      // write addresses of indexed terms
      termAddresses.finish();
      addressBuffer.writeTo(data);
      addressBuffer = null;
      termAddresses = null;
      meta.writeVInt(minLength);
      meta.writeVInt(maxLength);
      meta.writeVLong(count);
      meta.writeLong(startFP);
      meta.writeVInt(ADDRESS_INTERVAL);
      meta.writeLong(indexStartFP);
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeVInt(BLOCK_SIZE);
    }
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    checkCanWrite(field);
    meta.writeVInt(field.number);
    meta.writeByte(Lucene45DocValuesFormat.SORTED);
    addTermsDict(field, values);
    addNumericField(field, docToOrd, false);
  }
  
  @Override
  public void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException {
    throw new UnsupportedOperationException("Lucene 4.5 does not support SORTED_NUMERIC");
  }

  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, final Iterable<Number> docToOrdCount, final Iterable<Number> ords) throws IOException {
    checkCanWrite(field);
    meta.writeVInt(field.number);
    meta.writeByte(Lucene45DocValuesFormat.SORTED_SET);

    if (isSingleValued(docToOrdCount)) {
      meta.writeVInt(SORTED_SET_SINGLE_VALUED_SORTED);
      // The field is single-valued, we can encode it as SORTED
      addSortedField(field, values, singletonView(docToOrdCount, ords, -1L));
      return;
    }

    meta.writeVInt(SORTED_SET_WITH_ADDRESSES);

    // write the ord -> byte[] as a binary field
    addTermsDict(field, values);

    // write the stream of ords as a numeric field
    // NOTE: we could return an iterator that delta-encodes these within a doc
    addNumericField(field, ords, false);

    // write the doc -> ord count as a absolute index to the stream
    meta.writeVInt(field.number);
    meta.writeByte(Lucene45DocValuesFormat.NUMERIC);
    meta.writeVInt(DELTA_COMPRESSED);
    meta.writeLong(-1L);
    meta.writeVInt(PackedInts.VERSION_CURRENT);
    meta.writeLong(data.getFilePointer());
    meta.writeVLong(maxDoc);
    meta.writeVInt(BLOCK_SIZE);

    final MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(data, BLOCK_SIZE);
    long addr = 0;
    for (Number v : docToOrdCount) {
      addr += v.longValue();
      writer.add(addr);
    }
    writer.finish();
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeVInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data); // write checksum
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      meta = data = null;
    }
  }
  
  void checkCanWrite(FieldInfo field) {
    if ((field.getDocValuesType() == DocValuesType.NUMERIC || 
        field.getDocValuesType() == DocValuesType.BINARY) && 
        field.getDocValuesGen() != -1) {
      // ok
    } else {
      throw new UnsupportedOperationException("this codec can only be used for reading");
    }
  }
}
