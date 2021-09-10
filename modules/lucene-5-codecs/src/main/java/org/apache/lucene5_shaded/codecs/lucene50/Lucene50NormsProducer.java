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
package org.apache.lucene5_shaded.codecs.lucene50;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.NormsProducer;
import org.apache.lucene5_shaded.index.CorruptIndexException;
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
import org.apache.lucene5_shaded.util.SparseFixedBitSet;
import org.apache.lucene5_shaded.util.packed.BlockPackedReader;
import org.apache.lucene5_shaded.util.packed.MonotonicBlockPackedReader;
import org.apache.lucene5_shaded.util.packed.PackedInts;

/**
 * Reader for {@link Lucene50NormsFormat}
 * @deprecated Only for reading old 5.0-5.2 segments
 */
@Deprecated
final class Lucene50NormsProducer extends NormsProducer {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<String,NormsEntry> norms = new HashMap<>();
  private final IndexInput data;
  
  // ram instances we have already loaded
  final Map<String,Norms> instances = new HashMap<>();
  
  private final AtomicLong ramBytesUsed;
  private final AtomicInteger activeCount = new AtomicInteger();
  private final int maxDoc;
  
  private final boolean merging;
  
  // clone for merge: when merging we don't do any instances.put()s
  Lucene50NormsProducer(Lucene50NormsProducer original) {
    assert Thread.holdsLock(original);
    norms.putAll(original.norms);
    data = original.data.clone();
    instances.putAll(original.instances);
    ramBytesUsed = new AtomicLong(original.ramBytesUsed.get());
    activeCount.set(original.activeCount.get());
    maxDoc = original.maxDoc;
    merging = true;
  }
    
  Lucene50NormsProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    merging = false;
    maxDoc = state.segmentInfo.maxDoc();
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    int version = -1;
    
    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
      Throwable priorE = null;
      try {
        version = CodecUtil.checkIndexHeader(in, metaCodec, Lucene50NormsFormat.VERSION_START, Lucene50NormsFormat.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
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
      final int version2 = CodecUtil.checkIndexHeader(data, dataCodec, Lucene50NormsFormat.VERSION_START, Lucene50NormsFormat.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
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
      NormsEntry entry = readEntry(info, meta);
      norms.put(info.name, entry);
      fieldNumber = meta.readVInt();
    }
  }
  
  private NormsEntry readEntry(FieldInfo info, IndexInput meta) throws IOException {
    NormsEntry entry = new NormsEntry();
    entry.count = meta.readVInt();
    entry.format = meta.readByte();
    entry.offset = meta.readLong();
    switch(entry.format) {
      case Lucene50NormsFormat.CONST_COMPRESSED:
      case Lucene50NormsFormat.UNCOMPRESSED:
      case Lucene50NormsFormat.TABLE_COMPRESSED:
      case Lucene50NormsFormat.DELTA_COMPRESSED:
        break;
      case Lucene50NormsFormat.PATCHED_BITSET:
      case Lucene50NormsFormat.PATCHED_TABLE:
      case Lucene50NormsFormat.INDIRECT:
        if (meta.readVInt() != info.number) {
          throw new CorruptIndexException("indirect norms entry for field: " + info.name + " is corrupt", meta);
        }
        entry.nested = readEntry(info, meta);
        break;
      default:
        throw new CorruptIndexException("Unknown format: " + entry.format, meta);
    }
    return entry;
  }

  @Override
  public synchronized NumericDocValues getNorms(FieldInfo field) throws IOException {
    Norms instance = instances.get(field.name);
    if (instance == null) {
      instance = loadNorms(norms.get(field.name));
      if (!merging) {
        instances.put(field.name, instance);
        activeCount.incrementAndGet();
        ramBytesUsed.addAndGet(instance.ramBytesUsed());
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
    return Accountables.namedAccountables("field", instances);
  }
  
  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  private Norms loadNorms(NormsEntry entry) throws IOException {
    switch(entry.format) {
      case Lucene50NormsFormat.CONST_COMPRESSED: {
        final long v = entry.offset;
        return new Norms() {
          @Override
          public long get(int docID) {
            return v;
          }

          @Override
          public long ramBytesUsed() {
            return 8;
          }

          @Override
          public String toString() {
            return "constant";
          }
        };
      }
      case Lucene50NormsFormat.UNCOMPRESSED: {
        data.seek(entry.offset);
        final byte bytes[] = new byte[entry.count];
        data.readBytes(bytes, 0, bytes.length);
        return new Norms() {
          @Override
          public long get(int docID) {
            return bytes[docID];
          }

          @Override
          public long ramBytesUsed() {
            return RamUsageEstimator.sizeOf(bytes);
          }

          @Override
          public String toString() {
            return "byte array";
          }
        };
      }
      case Lucene50NormsFormat.DELTA_COMPRESSED: {
        data.seek(entry.offset);
        int packedIntsVersion = data.readVInt();
        int blockSize = data.readVInt();
        final BlockPackedReader reader = new BlockPackedReader(data, packedIntsVersion, blockSize, entry.count, false);
        return new Norms() {
          @Override
          public long get(int docID) {
            return reader.get(docID);
          }

          @Override
          public long ramBytesUsed() {
            return reader.ramBytesUsed();
          }

          @Override
          public Collection<Accountable> getChildResources() {
            return Collections.<Accountable>singleton(reader);
          }

          @Override
          public String toString() {
            return "delta compressed";
          }
        };
      }
      case Lucene50NormsFormat.TABLE_COMPRESSED: {
        data.seek(entry.offset);
        int packedIntsVersion = data.readVInt();
        final int formatID = data.readVInt();
        final int bitsPerValue = data.readVInt();
        
        if (bitsPerValue != 1 && bitsPerValue != 2 && bitsPerValue != 4) {
          throw new CorruptIndexException("TABLE_COMPRESSED only supports bpv=1, bpv=2 and bpv=4, got=" + bitsPerValue, data);
        }
        int size = 1 << bitsPerValue;
        final byte decode[] = new byte[size];
        final int ordsSize = data.readVInt();
        for (int i = 0; i < ordsSize; ++i) {
          decode[i] = data.readByte();
        }
        for (int i = ordsSize; i < size; ++i) {
          decode[i] = 0;
        }

        final PackedInts.Reader ordsReader = PackedInts.getReaderNoHeader(data, PackedInts.Format.byId(formatID), packedIntsVersion, entry.count, bitsPerValue);
        return new Norms() {
          @Override
          public long get(int docID) {
            return decode[(int)ordsReader.get(docID)];
          }
          
          @Override
          public long ramBytesUsed() {
            return RamUsageEstimator.sizeOf(decode) + ordsReader.ramBytesUsed();
          }

          @Override
          public Collection<Accountable> getChildResources() {
            return Collections.<Accountable>singleton(ordsReader);
          }

          @Override
          public String toString() {
            return "table compressed";
          }
        };
      }
      case Lucene50NormsFormat.INDIRECT: {
        data.seek(entry.offset);
        final long common = data.readLong();
        int packedIntsVersion = data.readVInt();
        int blockSize = data.readVInt();
        final MonotonicBlockPackedReader live = MonotonicBlockPackedReader.of(data, packedIntsVersion, blockSize, entry.count, false);
        final Norms nestedInstance = loadNorms(entry.nested);
        final int upperBound = entry.count-1;
        return new Norms() {
          @Override
          public long get(int docID) {
            int low = 0;
            int high = upperBound;
            while (low <= high) {
              int mid = (low + high) >>> 1;
              long doc = live.get(mid);
              
              if (doc < docID) {
                low = mid + 1;
              } else if (doc > docID) {
                high = mid - 1;
              } else {
                return nestedInstance.get(mid);
              }
            }
            return common;
          }

          @Override
          public long ramBytesUsed() {
            return live.ramBytesUsed() + nestedInstance.ramBytesUsed();
          }

          @Override
          public Collection<Accountable> getChildResources() {
            List<Accountable> children = new ArrayList<>();
            children.add(Accountables.namedAccountable("keys", live));
            children.add(Accountables.namedAccountable("values", nestedInstance));
            return Collections.unmodifiableList(children);
          }

          @Override
          public String toString() {
            return "indirect";
          }
        };
      }
      case Lucene50NormsFormat.PATCHED_BITSET: {
        data.seek(entry.offset);
        final long common = data.readLong();
        int packedIntsVersion = data.readVInt();
        int blockSize = data.readVInt();
        MonotonicBlockPackedReader live = MonotonicBlockPackedReader.of(data, packedIntsVersion, blockSize, entry.count, true);
        final SparseFixedBitSet set = new SparseFixedBitSet(maxDoc);
        for (int i = 0; i < live.size(); i++) {
          int doc = (int) live.get(i);
          set.set(doc);
        }
        final Norms nestedInstance = loadNorms(entry.nested);
        return new Norms() {
          @Override
          public long get(int docID) {
            if (set.get(docID)) {
              return nestedInstance.get(docID);
            } else {
              return common;
            }
          }
          
          @Override
          public long ramBytesUsed() {
            return set.ramBytesUsed() + nestedInstance.ramBytesUsed();
          }

          @Override
          public Collection<Accountable> getChildResources() {
            List<Accountable> children = new ArrayList<>();
            children.add(Accountables.namedAccountable("keys", set));
            children.add(Accountables.namedAccountable("values", nestedInstance));
            return Collections.unmodifiableList(children);
          }

          @Override
          public String toString() {
            return "patched bitset";
          }
        };
      }
      case Lucene50NormsFormat.PATCHED_TABLE: {
        data.seek(entry.offset);
        int packedIntsVersion = data.readVInt();
        final int formatID = data.readVInt();
        final int bitsPerValue = data.readVInt();

        if (bitsPerValue != 2 && bitsPerValue != 4) {
          throw new CorruptIndexException("PATCHED_TABLE only supports bpv=2 and bpv=4, got=" + bitsPerValue, data);
        }
        final int size = 1 << bitsPerValue;
        final int ordsSize = data.readVInt();
        final byte decode[] = new byte[ordsSize];
        assert ordsSize + 1 == size;
        for (int i = 0; i < ordsSize; ++i) {
          decode[i] = data.readByte();
        }
        
        final PackedInts.Reader ordsReader = PackedInts.getReaderNoHeader(data, PackedInts.Format.byId(formatID), packedIntsVersion, entry.count, bitsPerValue);
        final Norms nestedInstance = loadNorms(entry.nested);
        
        return new Norms() {
          @Override
          public long get(int docID) {
            int ord = (int)ordsReader.get(docID);
            try {
              // doing a try/catch here eliminates a seemingly unavoidable branch in hotspot...
              return decode[ord];
            } catch (IndexOutOfBoundsException e) {
              return nestedInstance.get(docID);
            }
          }

          @Override
          public long ramBytesUsed() {
            return RamUsageEstimator.sizeOf(decode) + ordsReader.ramBytesUsed() + nestedInstance.ramBytesUsed();
          }

          @Override
          public Collection<Accountable> getChildResources() {
            List<Accountable> children = new ArrayList<>();
            children.add(Accountables.namedAccountable("common", ordsReader));
            children.add(Accountables.namedAccountable("uncommon", nestedInstance));
            return Collections.unmodifiableList(children);
          }

          @Override
          public String toString() {
            return "patched table";
          }
        };
      }
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void close() throws IOException {
    data.close();
  }
  
  static class NormsEntry {
    byte format;
    long offset;
    int count;
    NormsEntry nested;
  }
  
  static abstract class Norms extends NumericDocValues implements Accountable {
    @Override
    public Collection<Accountable> getChildResources() {
      return Collections.emptyList();
    }
  }

  @Override
  public synchronized NormsProducer getMergeInstance() throws IOException {
    return new Lucene50NormsProducer(this);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + norms.size() + ",active=" + activeCount.get() + ")";
  }
}
