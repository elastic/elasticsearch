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

import java.io.Closeable; // javadocs
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.DocValuesProducer;
import org.apache.lucene5_shaded.index.BinaryDocValues;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.DocValues;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.RandomAccessOrds;
import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.index.SortedDocValues;
import org.apache.lucene5_shaded.index.SortedNumericDocValues;
import org.apache.lucene5_shaded.index.SortedSetDocValues;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.index.TermsEnum.SeekStatus;
import org.apache.lucene5_shaded.store.ChecksumIndexInput;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.store.RandomAccessInput;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.LongValues;
import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.packed.DirectReader;
import org.apache.lucene5_shaded.util.packed.MonotonicBlockPackedReader;

/** 
 * reader for 4.9 docvalues format
 * @deprecated only for 4.x segments 
 */
@Deprecated
class Lucene49DocValuesProducer extends DocValuesProducer implements Closeable {
  private final Map<String,NumericEntry> numerics = new HashMap<>();
  private final Map<String,BinaryEntry> binaries = new HashMap<>();
  private final Map<String,SortedSetEntry> sortedSets = new HashMap<>();
  private final Map<String,SortedSetEntry> sortedNumerics = new HashMap<>();
  private final Map<String,NumericEntry> ords = new HashMap<>();
  private final Map<String,NumericEntry> ordIndexes = new HashMap<>();
  private final AtomicLong ramBytesUsed;
  private final IndexInput data;
  private final int numFields;
  private final int maxDoc;

  // memory-resident structures
  private final Map<String,MonotonicBlockPackedReader> addressInstances = new HashMap<>();
  private final Map<String,MonotonicBlockPackedReader> ordIndexInstances = new HashMap<>();
  
  private final boolean merging;
  
  // clone for merge: when merging we don't do any instances.put()s
  Lucene49DocValuesProducer(Lucene49DocValuesProducer original) throws IOException {
    assert Thread.holdsLock(original);
    numerics.putAll(original.numerics);
    binaries.putAll(original.binaries);
    sortedSets.putAll(original.sortedSets);
    sortedNumerics.putAll(original.sortedNumerics);
    ords.putAll(original.ords);
    ordIndexes.putAll(original.ordIndexes);
    ramBytesUsed = new AtomicLong(original.ramBytesUsed());
    data = original.data.clone();
    numFields = original.numFields;
    maxDoc = original.maxDoc;
    addressInstances.putAll(original.addressInstances);
    ordIndexInstances.putAll(original.ordIndexInstances);
    merging = true;
  }
  
  /** expert: instantiates a new reader */
  Lucene49DocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    this.maxDoc = state.segmentInfo.maxDoc();
    merging = false;
    
    int version = -1;
    int numFields = -1;
    
    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
      Throwable priorE = null;
      try {
        version = CodecUtil.checkHeader(in, metaCodec, 
                                            Lucene49DocValuesFormat.VERSION_START,
                                            Lucene49DocValuesFormat.VERSION_CURRENT);
        numFields = readFields(in, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(in, priorE);
      }
    }
    this.numFields = numFields;

    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    this.data = state.directory.openInput(dataName, state.context);
    boolean success = false;
    try {
      final int version2 = CodecUtil.checkHeader(data, dataCodec, 
                                                 Lucene49DocValuesFormat.VERSION_START,
                                                 Lucene49DocValuesFormat.VERSION_CURRENT);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2, data);
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
    
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
  }

  private void readSortedField(FieldInfo info, IndexInput meta) throws IOException {
    // sorted = binary + numeric
    if (meta.readVInt() != info.number) {
      throw new CorruptIndexException("sorted entry for field: " + info.name + " is corrupt", meta);
    }
    if (meta.readByte() != Lucene49DocValuesFormat.BINARY) {
      throw new CorruptIndexException("sorted entry for field: " + info.name + " is corrupt", meta);
    }
    BinaryEntry b = readBinaryEntry(meta);
    binaries.put(info.name, b);
    
    if (meta.readVInt() != info.number) {
      throw new CorruptIndexException("sorted entry for field: " + info.name + " is corrupt", meta);
    }
    if (meta.readByte() != Lucene49DocValuesFormat.NUMERIC) {
      throw new CorruptIndexException("sorted entry for field: " + info.name + " is corrupt", meta);
    }
    NumericEntry n = readNumericEntry(meta);
    ords.put(info.name, n);
  }

  private void readSortedSetFieldWithAddresses(FieldInfo info, IndexInput meta) throws IOException {
    // sortedset = binary + numeric (addresses) + ordIndex
    if (meta.readVInt() != info.number) {
      throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
    }
    if (meta.readByte() != Lucene49DocValuesFormat.BINARY) {
      throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
    }
    BinaryEntry b = readBinaryEntry(meta);
    binaries.put(info.name, b);

    if (meta.readVInt() != info.number) {
      throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
    }
    if (meta.readByte() != Lucene49DocValuesFormat.NUMERIC) {
      throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
    }
    NumericEntry n1 = readNumericEntry(meta);
    ords.put(info.name, n1);

    if (meta.readVInt() != info.number) {
      throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
    }
    if (meta.readByte() != Lucene49DocValuesFormat.NUMERIC) {
      throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
    }
    NumericEntry n2 = readNumericEntry(meta);
    ordIndexes.put(info.name, n2);
  }

  private int readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int numFields = 0;
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      numFields++;
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        // trickier to validate more: because we use multiple entries for "composite" types like sortedset, etc.
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      byte type = meta.readByte();
      if (type == Lucene49DocValuesFormat.NUMERIC) {
        numerics.put(info.name, readNumericEntry(meta));
      } else if (type == Lucene49DocValuesFormat.BINARY) {
        BinaryEntry b = readBinaryEntry(meta);
        binaries.put(info.name, b);
      } else if (type == Lucene49DocValuesFormat.SORTED) {
        readSortedField(info, meta);
      } else if (type == Lucene49DocValuesFormat.SORTED_SET) {
        SortedSetEntry ss = readSortedSetEntry(meta);
        sortedSets.put(info.name, ss);
        if (ss.format == Lucene49DocValuesConsumer.SORTED_WITH_ADDRESSES) {
          readSortedSetFieldWithAddresses(info, meta);
        } else if (ss.format == Lucene49DocValuesConsumer.SORTED_SINGLE_VALUED) {
          if (meta.readVInt() != fieldNumber) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
          }
          if (meta.readByte() != Lucene49DocValuesFormat.SORTED) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
          }
          readSortedField(info, meta);
        } else {
          throw new AssertionError();
        }
      } else if (type == Lucene49DocValuesFormat.SORTED_NUMERIC) {
        SortedSetEntry ss = readSortedSetEntry(meta);
        sortedNumerics.put(info.name, ss);
        if (meta.readVInt() != fieldNumber) {
          throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
        }
        if (meta.readByte() != Lucene49DocValuesFormat.NUMERIC) {
          throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
        }
        numerics.put(info.name, readNumericEntry(meta));
        if (ss.format == Lucene49DocValuesConsumer.SORTED_WITH_ADDRESSES) {
          if (meta.readVInt() != fieldNumber) {
            throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
          }
          if (meta.readByte() != Lucene49DocValuesFormat.NUMERIC) {
            throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
          }
          NumericEntry ordIndex = readNumericEntry(meta);
          ordIndexes.put(info.name, ordIndex);
        } else if (ss.format != Lucene49DocValuesConsumer.SORTED_SINGLE_VALUED) {
          throw new AssertionError();
        }
      } else {
        throw new CorruptIndexException("invalid type: " + type, meta);
      }
      fieldNumber = meta.readVInt();
    }
    return numFields;
  }
  
  static NumericEntry readNumericEntry(IndexInput meta) throws IOException {
    NumericEntry entry = new NumericEntry();
    entry.format = meta.readVInt();
    entry.missingOffset = meta.readLong();
    entry.offset = meta.readLong();
    entry.count = meta.readVLong();
    switch(entry.format) {
      case Lucene49DocValuesConsumer.GCD_COMPRESSED:
        entry.minValue = meta.readLong();
        entry.gcd = meta.readLong();
        entry.bitsPerValue = meta.readVInt();
        break;
      case Lucene49DocValuesConsumer.TABLE_COMPRESSED:
        final int uniqueValues = meta.readVInt();
        if (uniqueValues > 256) {
          throw new CorruptIndexException("TABLE_COMPRESSED cannot have more than 256 distinct values, got=" + uniqueValues, meta);
        }
        entry.table = new long[uniqueValues];
        for (int i = 0; i < uniqueValues; ++i) {
          entry.table[i] = meta.readLong();
        }
        entry.bitsPerValue = meta.readVInt();
        break;
      case Lucene49DocValuesConsumer.DELTA_COMPRESSED:
        entry.minValue = meta.readLong();
        entry.bitsPerValue = meta.readVInt();
        break;
      case Lucene49DocValuesConsumer.MONOTONIC_COMPRESSED:
        entry.packedIntsVersion = meta.readVInt();
        entry.blockSize = meta.readVInt();
        break;
      default:
        throw new CorruptIndexException("Unknown format: " + entry.format, meta);
    }
    entry.endOffset = meta.readLong();
    return entry;
  }
  
  static BinaryEntry readBinaryEntry(IndexInput meta) throws IOException {
    BinaryEntry entry = new BinaryEntry();
    entry.format = meta.readVInt();
    entry.missingOffset = meta.readLong();
    entry.minLength = meta.readVInt();
    entry.maxLength = meta.readVInt();
    entry.count = meta.readVLong();
    entry.offset = meta.readLong();
    switch(entry.format) {
      case Lucene49DocValuesConsumer.BINARY_FIXED_UNCOMPRESSED:
        break;
      case Lucene49DocValuesConsumer.BINARY_PREFIX_COMPRESSED:
        entry.addressInterval = meta.readVInt();
        entry.addressesOffset = meta.readLong();
        entry.packedIntsVersion = meta.readVInt();
        entry.blockSize = meta.readVInt();
        break;
      case Lucene49DocValuesConsumer.BINARY_VARIABLE_UNCOMPRESSED:
        entry.addressesOffset = meta.readLong();
        entry.packedIntsVersion = meta.readVInt();
        entry.blockSize = meta.readVInt();
        break;
      default:
        throw new CorruptIndexException("Unknown format: " + entry.format, meta);
    }
    return entry;
  }

  SortedSetEntry readSortedSetEntry(IndexInput meta) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    entry.format = meta.readVInt();
    if (entry.format != Lucene49DocValuesConsumer.SORTED_SINGLE_VALUED && entry.format != Lucene49DocValuesConsumer.SORTED_WITH_ADDRESSES) {
      throw new CorruptIndexException("Unknown format: " + entry.format, meta);
    }
    return entry;
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericEntry entry = numerics.get(field.name);
    return getNumeric(entry);
  }
  
  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }
  
  @Override
  public synchronized Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    resources.addAll(Accountables.namedAccountables("addresses field", addressInstances));
    resources.addAll(Accountables.namedAccountables("ord index field", ordIndexInstances));
    return Collections.unmodifiableList(resources);
  }
  
  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + numFields + ")";
  }

  LongValues getNumeric(NumericEntry entry) throws IOException {
    RandomAccessInput slice = this.data.randomAccessSlice(entry.offset, entry.endOffset - entry.offset);
    switch (entry.format) {
      case Lucene49DocValuesConsumer.DELTA_COMPRESSED:
        final long delta = entry.minValue;
        final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
        return new LongValues() {
          @Override
          public long get(long id) {
            return delta + values.get(id);
          }
        };
      case Lucene49DocValuesConsumer.GCD_COMPRESSED:
        final long min = entry.minValue;
        final long mult = entry.gcd;
        final LongValues quotientReader = DirectReader.getInstance(slice, entry.bitsPerValue);
        return new LongValues() {
          @Override
          public long get(long id) {
            return min + mult * quotientReader.get(id);
          }
        };
      case Lucene49DocValuesConsumer.TABLE_COMPRESSED:
        final long table[] = entry.table;
        final LongValues ords = DirectReader.getInstance(slice, entry.bitsPerValue);
        return new LongValues() {
          @Override
          public long get(long id) {
            return table[(int) ords.get(id)];
          }
        };
      default:
        throw new AssertionError();
    }
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryEntry bytes = binaries.get(field.name);
    switch(bytes.format) {
      case Lucene49DocValuesConsumer.BINARY_FIXED_UNCOMPRESSED:
        return getFixedBinary(field, bytes);
      case Lucene49DocValuesConsumer.BINARY_VARIABLE_UNCOMPRESSED:
        return getVariableBinary(field, bytes);
      case Lucene49DocValuesConsumer.BINARY_PREFIX_COMPRESSED:
        return getCompressedBinary(field, bytes);
      default:
        throw new AssertionError();
    }
  }
  
  private BinaryDocValues getFixedBinary(FieldInfo field, final BinaryEntry bytes) {
    final IndexInput data = this.data.clone();

    return new LongBinaryDocValues() {
      final BytesRef term;
      {
        term = new BytesRef(bytes.maxLength);
        term.offset = 0;
        term.length = bytes.maxLength;
      }
      
      @Override
      public BytesRef get(long id) {
        long address = bytes.offset + id * bytes.maxLength;
        try {
          data.seek(address);
          data.readBytes(term.bytes, 0, term.length);
          return term;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
  
  /** returns an address instance for variable-length binary values. */
  private synchronized MonotonicBlockPackedReader getAddressInstance(IndexInput data, FieldInfo field, BinaryEntry bytes) throws IOException {
    final MonotonicBlockPackedReader addresses;
    MonotonicBlockPackedReader addrInstance = addressInstances.get(field.name);
    if (addrInstance == null) {
      data.seek(bytes.addressesOffset);
      addrInstance = MonotonicBlockPackedReader.of(data, bytes.packedIntsVersion, bytes.blockSize, bytes.count+1, false);
      if (!merging) {
        addressInstances.put(field.name, addrInstance);
        ramBytesUsed.addAndGet(addrInstance.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_INT);
      }
    }
    addresses = addrInstance;
    return addresses;
  }
  
  private BinaryDocValues getVariableBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
    final IndexInput data = this.data.clone();
    
    final MonotonicBlockPackedReader addresses = getAddressInstance(data, field, bytes);

    return new LongBinaryDocValues() {
      final BytesRef term = new BytesRef(Math.max(0, bytes.maxLength));
      
      @Override
      public BytesRef get(long id) {
        long startAddress = bytes.offset + addresses.get(id);
        long endAddress = bytes.offset + addresses.get(id+1);
        int length = (int) (endAddress - startAddress);
        try {
          data.seek(startAddress);
          data.readBytes(term.bytes, 0, length);
          term.length = length;
          return term;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
  
  /** returns an address instance for prefix-compressed binary values. */
  private synchronized MonotonicBlockPackedReader getIntervalInstance(IndexInput data, FieldInfo field, BinaryEntry bytes) throws IOException {
    final MonotonicBlockPackedReader addresses;
    final long interval = bytes.addressInterval;
    MonotonicBlockPackedReader addrInstance = addressInstances.get(field.name);
    if (addrInstance == null) {
      data.seek(bytes.addressesOffset);
      final long size;
      if (bytes.count % interval == 0) {
        size = bytes.count / interval;
      } else {
        size = 1L + bytes.count / interval;
      }
      addrInstance = MonotonicBlockPackedReader.of(data, bytes.packedIntsVersion, bytes.blockSize, size, false);
      if (!merging) {
        addressInstances.put(field.name, addrInstance);
        ramBytesUsed.addAndGet(addrInstance.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_INT);
      }
    }
    addresses = addrInstance;
    return addresses;
  }


  private BinaryDocValues getCompressedBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
    final IndexInput data = this.data.clone();

    final MonotonicBlockPackedReader addresses = getIntervalInstance(data, field, bytes);
    
    return new CompressedBinaryDocValues(bytes, addresses, data);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    final int valueCount = (int) binaries.get(field.name).count;
    final BinaryDocValues binary = getBinary(field);
    NumericEntry entry = ords.get(field.name);
    final LongValues ordinals = getNumeric(entry);
    
    return new SortedDocValues() {

      @Override
      public int getOrd(int docID) {
        return (int) ordinals.get(docID);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return binary.get(ord);
      }

      @Override
      public int getValueCount() {
        return valueCount;
      }

      @Override
      public int lookupTerm(BytesRef key) {
        if (binary instanceof CompressedBinaryDocValues) {
          return (int) ((CompressedBinaryDocValues)binary).lookupTerm(key);
        } else {
        return super.lookupTerm(key);
        }
      }

      @Override
      public TermsEnum termsEnum() {
        if (binary instanceof CompressedBinaryDocValues) {
          return ((CompressedBinaryDocValues)binary).getTermsEnum();
        } else {
          return super.termsEnum();
        }
      }
    };
  }
  
  /** returns an address instance for sortedset ordinal lists */
  private synchronized MonotonicBlockPackedReader getOrdIndexInstance(IndexInput data, FieldInfo field, NumericEntry entry) throws IOException {
    final MonotonicBlockPackedReader ordIndex;
    MonotonicBlockPackedReader ordIndexInstance = ordIndexInstances.get(field.name);
    if (ordIndexInstance == null) {
      data.seek(entry.offset);
      ordIndexInstance = MonotonicBlockPackedReader.of(data, entry.packedIntsVersion, entry.blockSize, entry.count+1, false);
      if (!merging) {
        ordIndexInstances.put(field.name, ordIndexInstance);
        ramBytesUsed.addAndGet(ordIndexInstance.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_INT);
      }
    }
    ordIndex = ordIndexInstance;
    return ordIndex;
  }
  
  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    SortedSetEntry ss = sortedNumerics.get(field.name);
    NumericEntry numericEntry = numerics.get(field.name);
    final LongValues values = getNumeric(numericEntry);
    if (ss.format == Lucene49DocValuesConsumer.SORTED_SINGLE_VALUED) {
      final Bits docsWithField = getMissingBits(numericEntry.missingOffset);
      return DocValues.singleton(values, docsWithField);
    } else if (ss.format == Lucene49DocValuesConsumer.SORTED_WITH_ADDRESSES) {
      final IndexInput data = this.data.clone();
      final MonotonicBlockPackedReader ordIndex = getOrdIndexInstance(data, field, ordIndexes.get(field.name));
      
      return new SortedNumericDocValues() {
        long startOffset;
        long endOffset;
        
        @Override
        public void setDocument(int doc) {
          startOffset = ordIndex.get(doc);
          endOffset = ordIndex.get(doc+1L);
        }

        @Override
        public long valueAt(int index) {
          return values.get(startOffset + index);
        }

        @Override
        public int count() {
          return (int) (endOffset - startOffset);
        }
      };
    } else {
      throw new AssertionError();
    }
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    SortedSetEntry ss = sortedSets.get(field.name);
    if (ss.format == Lucene49DocValuesConsumer.SORTED_SINGLE_VALUED) {
      final SortedDocValues values = getSorted(field);
      return DocValues.singleton(values);
    } else if (ss.format != Lucene49DocValuesConsumer.SORTED_WITH_ADDRESSES) {
      throw new AssertionError();
    }

    final IndexInput data = this.data.clone();
    final long valueCount = binaries.get(field.name).count;
    // we keep the byte[]s and list of ords on disk, these could be large
    final LongBinaryDocValues binary = (LongBinaryDocValues) getBinary(field);
    final LongValues ordinals = getNumeric(ords.get(field.name));
    // but the addresses to the ord stream are in RAM
    final MonotonicBlockPackedReader ordIndex = getOrdIndexInstance(data, field, ordIndexes.get(field.name));
    
    return new RandomAccessOrds() {
      long startOffset;
      long offset;
      long endOffset;
      
      @Override
      public long nextOrd() {
        if (offset == endOffset) {
          return NO_MORE_ORDS;
        } else {
          long ord = ordinals.get(offset);
          offset++;
          return ord;
        }
      }

      @Override
      public void setDocument(int docID) {
        startOffset = offset = ordIndex.get(docID);
        endOffset = ordIndex.get(docID+1L);
      }

      @Override
      public BytesRef lookupOrd(long ord) {
        return binary.get(ord);
      }

      @Override
      public long getValueCount() {
        return valueCount;
      }
      
      @Override
      public long lookupTerm(BytesRef key) {
        if (binary instanceof CompressedBinaryDocValues) {
          return ((CompressedBinaryDocValues)binary).lookupTerm(key);
        } else {
          return super.lookupTerm(key);
        }
      }

      @Override
      public TermsEnum termsEnum() {
        if (binary instanceof CompressedBinaryDocValues) {
          return ((CompressedBinaryDocValues)binary).getTermsEnum();
        } else {
          return super.termsEnum();
        }
      }

      @Override
      public long ordAt(int index) {
        return ordinals.get(startOffset + index);
      }

      @Override
      public int cardinality() {
        return (int) (endOffset - startOffset);
      }
    };
  }
  
  private Bits getMissingBits(final long offset) throws IOException {
    if (offset == -1) {
      return new Bits.MatchAllBits(maxDoc);
    } else {
      int length = (int) ((maxDoc + 7L) >>> 3);
      final RandomAccessInput in = data.randomAccessSlice(offset, length);
      return new Bits() {
        @Override
        public boolean get(int index) {
          try {
            return (in.readByte(index >> 3) & (1 << (index & 7))) != 0;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public int length() {
          return maxDoc;
        }
      };
    }
  }

  @Override
  public Bits getDocsWithField(FieldInfo field) throws IOException {
    switch(field.getDocValuesType()) {
      case SORTED_SET:
        return DocValues.docsWithValue(getSortedSet(field), maxDoc);
      case SORTED_NUMERIC:
        return DocValues.docsWithValue(getSortedNumeric(field), maxDoc);
      case SORTED:
        return DocValues.docsWithValue(getSorted(field), maxDoc);
      case BINARY:
        BinaryEntry be = binaries.get(field.name);
        return getMissingBits(be.missingOffset);
      case NUMERIC:
        NumericEntry ne = numerics.get(field.name);
        return getMissingBits(ne.missingOffset);
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void close() throws IOException {
    data.close();
  }
  
  @Override
  public synchronized DocValuesProducer getMergeInstance() throws IOException {
    return new Lucene49DocValuesProducer(this);
  }

  /** metadata entry for a numeric docvalues field */
  static class NumericEntry {
    private NumericEntry() {}
    /** offset to the bitset representing docsWithField, or -1 if no documents have missing values */
    long missingOffset;
    /** offset to the actual numeric values */
    public long offset;
    /** end offset to the actual numeric values */
    public long endOffset;
    /** bits per value used to pack the numeric values */
    public int bitsPerValue;

    int format;
    /** packed ints version used to encode these numerics */
    public int packedIntsVersion;
    /** count of values written */
    public long count;
    /** packed ints blocksize */
    public int blockSize;
    
    long minValue;
    long gcd;
    long table[];
  }
  
  /** metadata entry for a binary docvalues field */
  static class BinaryEntry {
    private BinaryEntry() {}
    /** offset to the bitset representing docsWithField, or -1 if no documents have missing values */
    long missingOffset;
    /** offset to the actual binary values */
    long offset;

    int format;
    /** count of values written */
    public long count;
    int minLength;
    int maxLength;
    /** offset to the addressing data that maps a value to its slice of the byte[] */
    public long addressesOffset;
    /** interval of shared prefix chunks (when using prefix-compressed binary) */
    public long addressInterval;
    /** packed ints version used to encode addressing information */
    public int packedIntsVersion;
    /** packed ints blocksize */
    public int blockSize;
  }

  /** metadata entry for a sorted-set docvalues field */
  static class SortedSetEntry {
    private SortedSetEntry() {}
    int format;
  }

  // internally we compose complex dv (sorted/sortedset) from other ones
  static abstract class LongBinaryDocValues extends BinaryDocValues {
    @Override
    public final BytesRef get(int docID) {
      return get((long)docID);
    }
    
    abstract BytesRef get(long id);
  }
  
  // in the compressed case, we add a few additional operations for
  // more efficient reverse lookup and enumeration
  static class CompressedBinaryDocValues extends LongBinaryDocValues {
    final BinaryEntry bytes;
    final long interval;
    final long numValues;
    final long numIndexValues;
    final MonotonicBlockPackedReader addresses;
    final IndexInput data;
    final TermsEnum termsEnum;
    
    public CompressedBinaryDocValues(BinaryEntry bytes, MonotonicBlockPackedReader addresses, IndexInput data) throws IOException {
      this.bytes = bytes;
      this.interval = bytes.addressInterval;
      this.addresses = addresses;
      this.data = data;
      this.numValues = bytes.count;
      this.numIndexValues = addresses.size();
      this.termsEnum = getTermsEnum(data);
    }
    
    @Override
    public BytesRef get(long id) {
      try {
        termsEnum.seekExact(id);
        return termsEnum.term();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    long lookupTerm(BytesRef key) {
      try {
        SeekStatus status = termsEnum.seekCeil(key);
        if (status == SeekStatus.END) {
          return -numValues-1;
        } else if (status == SeekStatus.FOUND) {
          return termsEnum.ord();
        } else {
          return -termsEnum.ord()-1;
        }
      } catch (IOException bogus) {
        throw new RuntimeException(bogus);
      }
    }
    
    TermsEnum getTermsEnum() {
      try {
        return getTermsEnum(data.clone());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    private TermsEnum getTermsEnum(final IndexInput input) throws IOException {
      input.seek(bytes.offset);
      
      return new TermsEnum() {
        private long currentOrd = -1;
        // TODO: maxLength is negative when all terms are merged away...
        private final BytesRef term = new BytesRef(bytes.maxLength < 0 ? 0 : bytes.maxLength);

        @Override
        public BytesRef next() throws IOException {
          if (++currentOrd >= numValues) {
            return null;
          } else {
            int start = input.readVInt();
            int suffix = input.readVInt();
            input.readBytes(term.bytes, start, suffix);
            term.length = start + suffix;
            return term;
          }
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
          // binary-search just the index values to find the block,
          // then scan within the block
          long low = 0;
          long high = numIndexValues-1;

          while (low <= high) {
            long mid = (low + high) >>> 1;
            seekExact(mid * interval);
            int cmp = term.compareTo(text);

            if (cmp < 0) {
              low = mid + 1;
            } else if (cmp > 0) {
              high = mid - 1;
            } else {
              // we got lucky, found an indexed term
              return SeekStatus.FOUND;
            }
          }
          
          if (numIndexValues == 0) {
            return SeekStatus.END;
          }
          
          // block before insertion point
          long block = low-1;
          seekExact(block < 0 ? -1 : block * interval);
          
          while (next() != null) {
            int cmp = term.compareTo(text);
            if (cmp == 0) {
              return SeekStatus.FOUND;
            } else if (cmp > 0) {
              return SeekStatus.NOT_FOUND;
            }
          }
          
          return SeekStatus.END;
        }

        @Override
        public void seekExact(long ord) throws IOException {
          long block = ord / interval;

          if (ord >= currentOrd && block == currentOrd / interval) {
            // seek within current block
          } else {
            // position before start of block
            currentOrd = ord - ord % interval - 1;
            input.seek(bytes.offset + addresses.get(block));
          }
          
          while (currentOrd < ord) {
            next();
          }
        }

        @Override
        public BytesRef term() throws IOException {
          return term;
        }

        @Override
        public long ord() throws IOException {
          return currentOrd;
        }

        @Override
        public int docFreq() throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public long totalTermFreq() throws IOException {
          return -1;
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
