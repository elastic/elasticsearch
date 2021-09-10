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
package org.apache.lucene5_shaded.codecs.lucene49;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.NormsProducer;
import org.apache.lucene5_shaded.codecs.UndeadNormsProducer;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.DocValues;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.store.ChecksumIndexInput;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.packed.BlockPackedReader;
import org.apache.lucene5_shaded.util.packed.PackedInts;

/**
 * Reader for 4.9 norms
 * @deprecated only for reading 4.9/4.10 indexes
 */
@Deprecated
final class Lucene49NormsProducer extends NormsProducer {
  static final byte DELTA_COMPRESSED = 0;
  static final byte TABLE_COMPRESSED = 1;
  static final byte CONST_COMPRESSED = 2;
  static final byte UNCOMPRESSED = 3;
  static final int BLOCK_SIZE = 16384;
  
  // metadata maps (just file pointers and minimal stuff)
  private final Map<String,NormsEntry> norms = new HashMap<>();
  private final IndexInput data;
  
  // ram instances we have already loaded
  final Map<String,NumericDocValues> instances = new HashMap<>();
  final Map<String,Accountable> instancesInfo = new HashMap<>();
  
  private final int maxDoc;
  private final AtomicLong ramBytesUsed;
  private final AtomicInteger activeCount = new AtomicInteger();
  
  private final boolean merging;
  
  // clone for merge: when merging we don't do any instances.put()s
  Lucene49NormsProducer(Lucene49NormsProducer original) {
    assert Thread.holdsLock(original);
    norms.putAll(original.norms);
    data = original.data.clone();
    instances.putAll(original.instances);
    instancesInfo.putAll(original.instancesInfo);
    maxDoc = original.maxDoc;
    ramBytesUsed = new AtomicLong(original.ramBytesUsed.get());
    activeCount.set(original.activeCount.get());
    merging = true;
  }
    
  Lucene49NormsProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.maxDoc();
    merging = false;
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    int version = -1;
    
    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
      Throwable priorE = null;
      try {
        version = CodecUtil.checkHeader(in, metaCodec, Lucene49NormsFormat.VERSION_START, Lucene49NormsFormat.VERSION_CURRENT);
        readFields(in, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(in, priorE);
      }
    }

    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    this.data = state.directory.openInput(dataName, state.context);
    boolean success = false;
    try {
      final int version2 = CodecUtil.checkHeader(data, dataCodec, Lucene49NormsFormat.VERSION_START, Lucene49NormsFormat.VERSION_CURRENT);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + version + ",data=" + version2, data);
      }
      
      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(data);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.data);
      }
    }
  }
  
  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      } else if (!info.hasNorms()) {
        throw new CorruptIndexException("Invalid field: " + info.name, meta);
      }
      NormsEntry entry = new NormsEntry();
      entry.format = meta.readByte();
      entry.offset = meta.readLong();
      switch(entry.format) {
        case CONST_COMPRESSED:
        case UNCOMPRESSED:
        case TABLE_COMPRESSED:
        case DELTA_COMPRESSED:
          break;
        default:
          throw new CorruptIndexException("Unknown format: " + entry.format, meta);
      }
      norms.put(info.name, entry);
      fieldNumber = meta.readVInt();
    }
  }

  @Override
  public synchronized NumericDocValues getNorms(FieldInfo field) throws IOException {
    if (UndeadNormsProducer.isUndead(field)) {
      // Bring undead norms back to life; this is set in Lucene46FieldInfosFormat, to emulate pre-5.0 undead norms
      return DocValues.emptyNumeric();
    }
    NumericDocValues instance = instances.get(field.name);
    if (instance == null) {
      instance = loadNorms(field);
      if (!merging) {
        instances.put(field.name, instance);
        activeCount.incrementAndGet();
      }
    }
    return instance;
  }
  
  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }
  
  @Override
  public synchronized Collection<Accountable> getChildResources() {
    return Accountables.namedAccountables("field", instancesInfo);
  }
  
  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  private NumericDocValues loadNorms(FieldInfo field) throws IOException {
    NormsEntry entry = norms.get(field.name);
    switch(entry.format) {
      case CONST_COMPRESSED:
        if (!merging) {
          instancesInfo.put(field.name, Accountables.namedAccountable("constant", 8));
          ramBytesUsed.addAndGet(8);
        }
        final long v = entry.offset;
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return v;
          }
        };
      case UNCOMPRESSED:
        data.seek(entry.offset);
        final byte bytes[] = new byte[maxDoc];
        data.readBytes(bytes, 0, bytes.length);
        if (!merging) {
          ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(bytes));
          instancesInfo.put(field.name, Accountables.namedAccountable("byte array", maxDoc));
        }
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return bytes[docID];
          }
        };
      case DELTA_COMPRESSED:
        data.seek(entry.offset);
        int packedIntsVersion = data.readVInt();
        int blockSize = data.readVInt();
        final BlockPackedReader reader = new BlockPackedReader(data, packedIntsVersion, blockSize, maxDoc, false);
        if (!merging) {
          ramBytesUsed.addAndGet(reader.ramBytesUsed());
          instancesInfo.put(field.name, Accountables.namedAccountable("delta compressed", reader));
        }
        return reader;
      case TABLE_COMPRESSED:
        data.seek(entry.offset);
        int packedVersion = data.readVInt();
        int size = data.readVInt();
        if (size > 256) {
          throw new CorruptIndexException("TABLE_COMPRESSED cannot have more than 256 distinct values, got=" + size, data);
        }
        final long decode[] = new long[size];
        for (int i = 0; i < decode.length; i++) {
          decode[i] = data.readLong();
        }
        final int formatID = data.readVInt();
        final int bitsPerValue = data.readVInt();
        final PackedInts.Reader ordsReader = PackedInts.getReaderNoHeader(data, PackedInts.Format.byId(formatID), packedVersion, maxDoc, bitsPerValue);
        if (!merging) {
          ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(decode) + ordsReader.ramBytesUsed());
          instancesInfo.put(field.name, Accountables.namedAccountable("table compressed", ordsReader));
        }
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return decode[(int)ordsReader.get(docID)];
          }
        };
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void close() throws IOException {
    data.close();
  }
  
  @Override
  public synchronized NormsProducer getMergeInstance() throws IOException {
    return new Lucene49NormsProducer(this);
  }

  static class NormsEntry {
    byte format;
    long offset;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + norms.size() + ",active=" + activeCount.get() + ")";
  }
}
