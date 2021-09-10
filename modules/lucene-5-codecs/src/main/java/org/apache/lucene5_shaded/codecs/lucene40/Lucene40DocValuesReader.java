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
package org.apache.lucene5_shaded.codecs.lucene40;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.DocValuesProducer;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40FieldInfosFormat.LegacyDocValuesType;
import org.apache.lucene5_shaded.index.BinaryDocValues;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.index.SortedDocValues;
import org.apache.lucene5_shaded.index.SortedNumericDocValues;
import org.apache.lucene5_shaded.index.SortedSetDocValues;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.PagedBytes;
import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.packed.PackedInts;

/**
 * Reads the 4.0 format of norms/docvalues
 * @deprecated Only for reading old 4.0 and 4.1 segments
 */
@Deprecated
final class Lucene40DocValuesReader extends DocValuesProducer {
  private final Directory dir;
  private final SegmentReadState state;
  private final String legacyKey;
  private static final String segmentSuffix = "dv";

  // ram instances we have already loaded
  private final Map<String,NumericDocValues> numericInstances = new HashMap<>();
  private final Map<String,BinaryDocValues> binaryInstances = new HashMap<>();
  private final Map<String,SortedDocValues> sortedInstances = new HashMap<>();
  
  private final Map<String,Accountable> instanceInfo = new HashMap<>();

  private final AtomicLong ramBytesUsed;
  
  private final boolean merging;
  
  // clone for merge: when merging we don't do any instances.put()s
  Lucene40DocValuesReader(Lucene40DocValuesReader original) throws IOException {
    assert Thread.holdsLock(original);
    dir = original.dir;
    state = original.state;
    legacyKey = original.legacyKey;
    numericInstances.putAll(original.numericInstances);
    binaryInstances.putAll(original.binaryInstances);
    sortedInstances.putAll(original.sortedInstances);
    instanceInfo.putAll(original.instanceInfo);
    ramBytesUsed = new AtomicLong(original.ramBytesUsed.get());
    merging = true;
  }

  Lucene40DocValuesReader(SegmentReadState state, String filename, String legacyKey) throws IOException {
    this.state = state;
    this.legacyKey = legacyKey;
    this.dir = new Lucene40CompoundReader(state.directory, filename, state.context, false);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOf(getClass()));
    merging = false;
  }

  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericDocValues instance = numericInstances.get(field.name);
    if (instance == null) {
      String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
      IndexInput input = dir.openInput(fileName, state.context);
      boolean success = false;
      try {
        switch(LegacyDocValuesType.valueOf(field.getAttribute(legacyKey))) {
          case VAR_INTS:
            instance = loadVarIntsField(field, input);
            break;
          case FIXED_INTS_8:
            instance = loadByteField(field, input);
            break;
          case FIXED_INTS_16:
            instance = loadShortField(field, input);
            break;
          case FIXED_INTS_32:
            instance = loadIntField(field, input);
            break;
          case FIXED_INTS_64:
            instance = loadLongField(field, input);
            break;
          case FLOAT_32:
            instance = loadFloatField(field, input);
            break;
          case FLOAT_64:
            instance = loadDoubleField(field, input);
            break;
          default:
            throw new AssertionError();
        }
        CodecUtil.checkEOF(input);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(input);
        } else {
          IOUtils.closeWhileHandlingException(input);
        }
      }
      if (!merging) {
        numericInstances.put(field.name, instance);
      }
    }
    return instance;
  }

  private NumericDocValues loadVarIntsField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.VAR_INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.VAR_INTS_VERSION_START,
                                 Lucene40DocValuesFormat.VAR_INTS_VERSION_CURRENT);
    byte header = input.readByte();
    if (header == Lucene40DocValuesFormat.VAR_INTS_FIXED_64) {
      int maxDoc = state.segmentInfo.maxDoc();
      final long values[] = new long[maxDoc];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readLong();
      }
      long bytesUsed = RamUsageEstimator.sizeOf(values);
      if (!merging) {
        instanceInfo.put(field.name, Accountables.namedAccountable("long array", bytesUsed));
        ramBytesUsed.addAndGet(bytesUsed);
      }
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return values[docID];
        }
      };
    } else if (header == Lucene40DocValuesFormat.VAR_INTS_PACKED) {
      final long minValue = input.readLong();
      final long defaultValue = input.readLong();
      final PackedInts.Reader reader = PackedInts.getReader(input);
      if (!merging) {
        instanceInfo.put(field.name, reader);
        ramBytesUsed.addAndGet(reader.ramBytesUsed());
      }
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          final long value = reader.get(docID);
          if (value == defaultValue) {
            return 0;
          } else {
            return minValue + value;
          }
        }
      };
    } else {
      throw new CorruptIndexException("invalid VAR_INTS header byte: " + header, input);
    }
  }

  private NumericDocValues loadByteField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.INTS_VERSION_START,
                                 Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 1) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize, input);
    }
    int maxDoc = state.segmentInfo.maxDoc();
    final byte values[] = new byte[maxDoc];
    input.readBytes(values, 0, values.length);
    long bytesUsed = RamUsageEstimator.sizeOf(values);
    if (!merging) {
      instanceInfo.put(field.name, Accountables.namedAccountable("byte array", bytesUsed));
      ramBytesUsed.addAndGet(bytesUsed);
    }
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadShortField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.INTS_VERSION_START,
                                 Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 2) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize, input);
    }
    int maxDoc = state.segmentInfo.maxDoc();
    final short values[] = new short[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readShort();
    }
    long bytesUsed = RamUsageEstimator.sizeOf(values);
    if (!merging) {
      instanceInfo.put(field.name, Accountables.namedAccountable("short array", bytesUsed));
      ramBytesUsed.addAndGet(bytesUsed);
    }
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadIntField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.INTS_VERSION_START,
                                 Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 4) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize, input);
    }
    int maxDoc = state.segmentInfo.maxDoc();
    final int values[] = new int[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readInt();
    }
    long bytesUsed = RamUsageEstimator.sizeOf(values);
    if (!merging) {
      instanceInfo.put(field.name, Accountables.namedAccountable("int array", bytesUsed));
      ramBytesUsed.addAndGet(bytesUsed);
    }
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadLongField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.INTS_VERSION_START,
                                 Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 8) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize, input);
    }
    int maxDoc = state.segmentInfo.maxDoc();
    final long values[] = new long[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readLong();
    }
    long bytesUsed = RamUsageEstimator.sizeOf(values);
    if (!merging) {
      instanceInfo.put(field.name, Accountables.namedAccountable("long array", bytesUsed));
      ramBytesUsed.addAndGet(bytesUsed);
    }
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadFloatField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.FLOATS_CODEC_NAME,
                                 Lucene40DocValuesFormat.FLOATS_VERSION_START,
                                 Lucene40DocValuesFormat.FLOATS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 4) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize, input);
    }
    int maxDoc = state.segmentInfo.maxDoc();
    final int values[] = new int[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readInt();
    }
    long bytesUsed = RamUsageEstimator.sizeOf(values);
    if (!merging) {
      instanceInfo.put(field.name, Accountables.namedAccountable("float array", bytesUsed));
      ramBytesUsed.addAndGet(bytesUsed);
    }
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadDoubleField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.FLOATS_CODEC_NAME,
                                 Lucene40DocValuesFormat.FLOATS_VERSION_START,
                                 Lucene40DocValuesFormat.FLOATS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 8) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize, input);
    }
    int maxDoc = state.segmentInfo.maxDoc();
    final long values[] = new long[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readLong();
    }
    long bytesUsed = RamUsageEstimator.sizeOf(values);
    if (!merging) {
      instanceInfo.put(field.name, Accountables.namedAccountable("double array", bytesUsed));
      ramBytesUsed.addAndGet(bytesUsed);
    }
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryDocValues instance = binaryInstances.get(field.name);
    if (instance == null) {
      switch(LegacyDocValuesType.valueOf(field.getAttribute(legacyKey))) {
        case BYTES_FIXED_STRAIGHT:
          instance = loadBytesFixedStraight(field);
          break;
        case BYTES_VAR_STRAIGHT:
          instance = loadBytesVarStraight(field);
          break;
        case BYTES_FIXED_DEREF:
          instance = loadBytesFixedDeref(field);
          break;
        case BYTES_VAR_DEREF:
          instance = loadBytesVarDeref(field);
          break;
        default:
          throw new AssertionError();
      }
      if (!merging) {
        binaryInstances.put(field.name, instance);
      }
    }
    return instance;
  }

  private BinaryDocValues loadBytesFixedStraight(FieldInfo field) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
    IndexInput input = dir.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40DocValuesFormat.BYTES_FIXED_STRAIGHT_CODEC_NAME,
                                   Lucene40DocValuesFormat.BYTES_FIXED_STRAIGHT_VERSION_START,
                                   Lucene40DocValuesFormat.BYTES_FIXED_STRAIGHT_VERSION_CURRENT);
      final int fixedLength = input.readInt();
      PagedBytes bytes = new PagedBytes(16);
      bytes.copy(input, fixedLength * (long)state.segmentInfo.maxDoc());
      final PagedBytes.Reader bytesReader = bytes.freeze(true);
      CodecUtil.checkEOF(input);
      success = true;
      if (!merging) {
        ramBytesUsed.addAndGet(bytesReader.ramBytesUsed());
        instanceInfo.put(field.name, bytesReader);
      }
      return new BinaryDocValues() {

        @Override
        public BytesRef get(int docID) {
          final BytesRef term = new BytesRef();
          bytesReader.fillSlice(term, fixedLength * (long)docID, fixedLength);
          return term;
        }
      };
    } finally {
      if (success) {
        IOUtils.close(input);
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }

  private BinaryDocValues loadBytesVarStraight(FieldInfo field) throws IOException {
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
    String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
    IndexInput data = null;
    IndexInput index = null;
    boolean success = false;
    try {
      data = dir.openInput(dataName, state.context);
      CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_CODEC_NAME_DAT,
                                  Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_START,
                                  Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_CURRENT);
      index = dir.openInput(indexName, state.context);
      CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_CODEC_NAME_IDX,
                                   Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_START,
                                   Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_CURRENT);
      long totalBytes = index.readVLong();
      PagedBytes bytes = new PagedBytes(16);
      bytes.copy(data, totalBytes);
      final PagedBytes.Reader bytesReader = bytes.freeze(true);
      final PackedInts.Reader reader = PackedInts.getReader(index);
      CodecUtil.checkEOF(data);
      CodecUtil.checkEOF(index);
      success = true;
      long bytesUsed = bytesReader.ramBytesUsed() + reader.ramBytesUsed();
      if (!merging) {
        ramBytesUsed.addAndGet(bytesUsed);
        instanceInfo.put(field.name, Accountables.namedAccountable("variable straight", bytesUsed));
      }
      return new BinaryDocValues() {
        @Override
        public BytesRef get(int docID) {
          final BytesRef term = new BytesRef();
          long startAddress = reader.get(docID);
          long endAddress = reader.get(docID+1);
          bytesReader.fillSlice(term, startAddress, (int)(endAddress - startAddress));
          return term;
        }
      };
    } finally {
      if (success) {
        IOUtils.close(data, index);
      } else {
        IOUtils.closeWhileHandlingException(data, index);
      }
    }
  }

  private BinaryDocValues loadBytesFixedDeref(FieldInfo field) throws IOException {
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
    String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
    IndexInput data = null;
    IndexInput index = null;
    boolean success = false;
    try {
      data = dir.openInput(dataName, state.context);
      CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_FIXED_DEREF_CODEC_NAME_DAT,
                                  Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_START,
                                  Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_CURRENT);
      index = dir.openInput(indexName, state.context);
      CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_FIXED_DEREF_CODEC_NAME_IDX,
                                   Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_START,
                                   Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_CURRENT);

      final int fixedLength = data.readInt();
      final int valueCount = index.readInt();
      PagedBytes bytes = new PagedBytes(16);
      bytes.copy(data, fixedLength * (long) valueCount);
      final PagedBytes.Reader bytesReader = bytes.freeze(true);
      final PackedInts.Reader reader = PackedInts.getReader(index);
      CodecUtil.checkEOF(data);
      CodecUtil.checkEOF(index);
      long bytesUsed = bytesReader.ramBytesUsed() + reader.ramBytesUsed();
      if (!merging) {
        ramBytesUsed.addAndGet(bytesUsed);
        instanceInfo.put(field.name, Accountables.namedAccountable("fixed deref", bytesUsed));
      }
      success = true;
      return new BinaryDocValues() {
        @Override
        public BytesRef get(int docID) {
          final BytesRef term = new BytesRef();
          final long offset = fixedLength * reader.get(docID);
          bytesReader.fillSlice(term, offset, fixedLength);
          return term;
        }
      };
    } finally {
      if (success) {
        IOUtils.close(data, index);
      } else {
        IOUtils.closeWhileHandlingException(data, index);
      }
    }
  }

  private BinaryDocValues loadBytesVarDeref(FieldInfo field) throws IOException {
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
    String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
    IndexInput data = null;
    IndexInput index = null;
    boolean success = false;
    try {
      data = dir.openInput(dataName, state.context);
      CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_VAR_DEREF_CODEC_NAME_DAT,
                                  Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_START,
                                  Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_CURRENT);
      index = dir.openInput(indexName, state.context);
      CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_VAR_DEREF_CODEC_NAME_IDX,
                                   Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_START,
                                   Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_CURRENT);

      final long totalBytes = index.readLong();
      final PagedBytes bytes = new PagedBytes(16);
      bytes.copy(data, totalBytes);
      final PagedBytes.Reader bytesReader = bytes.freeze(true);
      final PackedInts.Reader reader = PackedInts.getReader(index);
      CodecUtil.checkEOF(data);
      CodecUtil.checkEOF(index);
      long bytesUsed = bytesReader.ramBytesUsed() + reader.ramBytesUsed();
      if (!merging) {
        ramBytesUsed.addAndGet(bytesUsed);
        instanceInfo.put(field.name, Accountables.namedAccountable("variable deref", bytesUsed));
      }
      success = true;
      return new BinaryDocValues() {
        
        @Override
        public BytesRef get(int docID) {
          final BytesRef term = new BytesRef();
          long startAddress = reader.get(docID);
          BytesRef lengthBytes = new BytesRef();
          bytesReader.fillSlice(lengthBytes, startAddress, 1);
          byte code = lengthBytes.bytes[lengthBytes.offset];
          if ((code & 128) == 0) {
            // length is 1 byte
            bytesReader.fillSlice(term, startAddress + 1, (int) code);
          } else {
            bytesReader.fillSlice(lengthBytes, startAddress + 1, 1);
            int length = ((code & 0x7f) << 8) | (lengthBytes.bytes[lengthBytes.offset] & 0xff);
            bytesReader.fillSlice(term, startAddress + 2, length);
          }
          return term;
        }
      };
    } finally {
      if (success) {
        IOUtils.close(data, index);
      } else {
        IOUtils.closeWhileHandlingException(data, index);
      }
    }
  }

  @Override
  public synchronized SortedDocValues getSorted(FieldInfo field) throws IOException {
    SortedDocValues instance = sortedInstances.get(field.name);
    if (instance == null) {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
      String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
      IndexInput data = null;
      IndexInput index = null;
      boolean success = false;
      try {
        data = dir.openInput(dataName, state.context);
        index = dir.openInput(indexName, state.context);
        switch(LegacyDocValuesType.valueOf(field.getAttribute(legacyKey))) {
          case BYTES_FIXED_SORTED:
            instance = loadBytesFixedSorted(field, data, index);
            break;
          case BYTES_VAR_SORTED:
            instance = loadBytesVarSorted(field, data, index);
            break;
          default:
            throw new AssertionError();
        }
        CodecUtil.checkEOF(data);
        CodecUtil.checkEOF(index);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(data, index);
        } else {
          IOUtils.closeWhileHandlingException(data, index);
        }
      }
      if (!merging) {
        sortedInstances.put(field.name, instance);
      }
    }
    return instance;
  }

  private SortedDocValues loadBytesFixedSorted(FieldInfo field, IndexInput data, IndexInput index) throws IOException {
    CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_FIXED_SORTED_CODEC_NAME_DAT,
                                Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_START,
                                Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_CURRENT);
    CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_FIXED_SORTED_CODEC_NAME_IDX,
                                 Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_START,
                                 Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_CURRENT);

    final int fixedLength = data.readInt();
    final int valueCount = index.readInt();

    PagedBytes bytes = new PagedBytes(16);
    bytes.copy(data, fixedLength * (long) valueCount);
    final PagedBytes.Reader bytesReader = bytes.freeze(true);
    final PackedInts.Reader reader = PackedInts.getReader(index);
    long bytesUsed = bytesReader.ramBytesUsed() + reader.ramBytesUsed();
    if (!merging) {
      ramBytesUsed.addAndGet(bytesUsed);
      instanceInfo.put(field.name, Accountables.namedAccountable("fixed sorted", bytesUsed));
    }

    return correctBuggyOrds(new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return (int) reader.get(docID);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        final BytesRef term = new BytesRef();
        bytesReader.fillSlice(term, fixedLength * (long) ord, fixedLength);
        return term;
      }

      @Override
      public int getValueCount() {
        return valueCount;
      }
    });
  }

  private SortedDocValues loadBytesVarSorted(FieldInfo field, IndexInput data, IndexInput index) throws IOException {
    CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_VAR_SORTED_CODEC_NAME_DAT,
                                Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_START,
                                Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_CURRENT);
    CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_VAR_SORTED_CODEC_NAME_IDX,
                                 Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_START,
                                 Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_CURRENT);

    long maxAddress = index.readLong();
    PagedBytes bytes = new PagedBytes(16);
    bytes.copy(data, maxAddress);
    final PagedBytes.Reader bytesReader = bytes.freeze(true);
    final PackedInts.Reader addressReader = PackedInts.getReader(index);
    final PackedInts.Reader ordsReader = PackedInts.getReader(index);

    final int valueCount = addressReader.size() - 1;
    long bytesUsed = bytesReader.ramBytesUsed() + addressReader.ramBytesUsed() + ordsReader.ramBytesUsed();
    if (!merging) {
      ramBytesUsed.addAndGet(bytesUsed);
      instanceInfo.put(field.name, Accountables.namedAccountable("var sorted", bytesUsed));
    }

    return correctBuggyOrds(new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return (int)ordsReader.get(docID);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        final BytesRef term = new BytesRef();
        long startAddress = addressReader.get(ord);
        long endAddress = addressReader.get(ord+1);
        bytesReader.fillSlice(term, startAddress, (int)(endAddress - startAddress));
        return term;
      }

      @Override
      public int getValueCount() {
        return valueCount;
      }
    });
  }

  // detects and corrects LUCENE-4717 in old indexes
  private SortedDocValues correctBuggyOrds(final SortedDocValues in) {
    final int maxDoc = state.segmentInfo.maxDoc();
    for (int i = 0; i < maxDoc; i++) {
      if (in.getOrd(i) == 0) {
        return in; // ok
      }
    }

    // we had ord holes, return an ord-shifting-impl that corrects the bug
    return new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return in.getOrd(docID) - 1;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return in.lookupOrd(ord+1);
      }

      @Override
      public int getValueCount() {
        return in.getValueCount() - 1;
      }
    };
  }
  
  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    throw new IllegalStateException("Lucene 4.0 does not support SortedNumeric: how did you pull this off?");
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    throw new IllegalStateException("Lucene 4.0 does not support SortedSet: how did you pull this off?");
  }

  @Override
  public Bits getDocsWithField(FieldInfo field) throws IOException {
    return new Bits.MatchAllBits(state.segmentInfo.maxDoc());
  }

  @Override
  public void close() throws IOException {
    dir.close();
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }
  
  @Override
  public synchronized Collection<Accountable> getChildResources() {
    return Accountables.namedAccountables("field", instanceInfo);
  }

  @Override
  public void checkIntegrity() throws IOException {
  }

  @Override
  public synchronized DocValuesProducer getMergeInstance() throws IOException {
    return new Lucene40DocValuesReader(this);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
