/*
 * @notice
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
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene54;

import org.apache.lucene.backward_codecs.packed.LegacyDirectMonotonicReader;
import org.apache.lucene.backward_codecs.packed.LegacyDirectReader;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.lucene.bwc.codecs.index.LegacyBinaryDocValues;
import org.elasticsearch.xpack.lucene.bwc.codecs.index.LegacyBinaryDocValuesWrapper;
import org.elasticsearch.xpack.lucene.bwc.codecs.index.LegacySortedSetDocValues;
import org.elasticsearch.xpack.lucene.bwc.codecs.index.LegacySortedSetDocValuesWrapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesConsumer.NumberType.ORDINAL;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesConsumer.NumberType.VALUE;

/** reader for {@link Lucene54DocValuesFormat} */
final class Lucene54DocValuesProducer extends DocValuesProducer implements Closeable {
    private final Map<String, NumericEntry> numerics = new HashMap<>();
    private final Map<String, BinaryEntry> binaries = new HashMap<>();
    private final Map<String, SortedSetEntry> sortedSets = new HashMap<>();
    private final Map<String, SortedSetEntry> sortedNumerics = new HashMap<>();
    private final Map<String, NumericEntry> ords = new HashMap<>();
    private final Map<String, NumericEntry> ordIndexes = new HashMap<>();
    private final int numFields;
    private final AtomicLong ramBytesUsed;
    private final IndexInput data;
    private final int maxDoc;

    // memory-resident structures
    private final Map<String, MonotonicBlockPackedReader> addressInstances = new HashMap<>();
    private final Map<String, ReverseTermsIndex> reverseIndexInstances = new HashMap<>();
    private final Map<String, LegacyDirectMonotonicReader.Meta> directAddressesMeta = new HashMap<>();

    private final boolean merging;

    // clone for merge: when merging we don't do any instances.put()s
    Lucene54DocValuesProducer(Lucene54DocValuesProducer original) {
        assert Thread.holdsLock(original);
        numerics.putAll(original.numerics);
        binaries.putAll(original.binaries);
        sortedSets.putAll(original.sortedSets);
        sortedNumerics.putAll(original.sortedNumerics);
        ords.putAll(original.ords);
        ordIndexes.putAll(original.ordIndexes);
        numFields = original.numFields;
        ramBytesUsed = new AtomicLong(original.ramBytesUsed.get());
        data = original.data.clone();
        maxDoc = original.maxDoc;

        addressInstances.putAll(original.addressInstances);
        reverseIndexInstances.putAll(original.reverseIndexInstances);
        merging = true;
    }

    /** expert: instantiates a new reader */
    Lucene54DocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        this.maxDoc = state.segmentInfo.maxDoc();
        merging = false;
        ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));

        int version = -1;
        int numFields = -1;

        // read in the entries from the metadata file.
        try (ChecksumIndexInput in = EndiannessReverserUtil.openChecksumInput(state.directory, metaName, state.context)) {
            Throwable priorE = null;
            try {
                version = CodecUtil.checkIndexHeader(
                    in,
                    metaCodec,
                    Lucene54DocValuesFormat.VERSION_START,
                    Lucene54DocValuesFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                numFields = readFields(in, state.fieldInfos);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(in, priorE);
            }
        }

        this.numFields = numFields;
        String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        this.data = EndiannessReverserUtil.openInput(state.directory, dataName, state.context);
        boolean success = false;
        try {
            final int version2 = CodecUtil.checkIndexHeader(
                data,
                dataCodec,
                Lucene54DocValuesFormat.VERSION_START,
                Lucene54DocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
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
            if (success == false) {
                IOUtils.closeWhileHandlingException(this.data);
            }
        }
    }

    private void readSortedField(FieldInfo info, IndexInput meta) throws IOException {
        // sorted = binary + numeric
        if (meta.readVInt() != info.number) {
            throw new CorruptIndexException("sorted entry for field: " + info.name + " is corrupt", meta);
        }
        if (meta.readByte() != Lucene54DocValuesFormat.BINARY) {
            throw new CorruptIndexException("sorted entry for field: " + info.name + " is corrupt", meta);
        }
        BinaryEntry b = readBinaryEntry(info, meta);
        binaries.put(info.name, b);

        if (meta.readVInt() != info.number) {
            throw new CorruptIndexException("sorted entry for field: " + info.name + " is corrupt", meta);
        }
        if (meta.readByte() != Lucene54DocValuesFormat.NUMERIC) {
            throw new CorruptIndexException("sorted entry for field: " + info.name + " is corrupt", meta);
        }
        NumericEntry n = readNumericEntry(info, meta);
        ords.put(info.name, n);
    }

    private void readSortedSetFieldWithAddresses(FieldInfo info, IndexInput meta) throws IOException {
        // sortedset = binary + numeric (addresses) + ordIndex
        if (meta.readVInt() != info.number) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }
        if (meta.readByte() != Lucene54DocValuesFormat.BINARY) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }
        BinaryEntry b = readBinaryEntry(info, meta);
        binaries.put(info.name, b);

        if (meta.readVInt() != info.number) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }
        if (meta.readByte() != Lucene54DocValuesFormat.NUMERIC) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }
        NumericEntry n1 = readNumericEntry(info, meta);
        ords.put(info.name, n1);

        if (meta.readVInt() != info.number) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }
        if (meta.readByte() != Lucene54DocValuesFormat.NUMERIC) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }
        NumericEntry n2 = readNumericEntry(info, meta);
        ordIndexes.put(info.name, n2);
    }

    private void readSortedSetFieldWithTable(FieldInfo info, IndexInput meta) throws IOException {
        // sortedset table = binary + ordset table + ordset index
        if (meta.readVInt() != info.number) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }
        if (meta.readByte() != Lucene54DocValuesFormat.BINARY) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }

        BinaryEntry b = readBinaryEntry(info, meta);
        binaries.put(info.name, b);

        if (meta.readVInt() != info.number) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }
        if (meta.readByte() != Lucene54DocValuesFormat.NUMERIC) {
            throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
        }
        NumericEntry n = readNumericEntry(info, meta);
        ords.put(info.name, n);
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
            if (type == Lucene54DocValuesFormat.NUMERIC) {
                numerics.put(info.name, readNumericEntry(info, meta));
            } else if (type == Lucene54DocValuesFormat.BINARY) {
                BinaryEntry b = readBinaryEntry(info, meta);
                binaries.put(info.name, b);
            } else if (type == Lucene54DocValuesFormat.SORTED) {
                readSortedField(info, meta);
            } else if (type == Lucene54DocValuesFormat.SORTED_SET) {
                SortedSetEntry ss = readSortedSetEntry(meta);
                sortedSets.put(info.name, ss);
                if (ss.format == Lucene54DocValuesFormat.SORTED_WITH_ADDRESSES) {
                    readSortedSetFieldWithAddresses(info, meta);
                } else if (ss.format == Lucene54DocValuesFormat.SORTED_SET_TABLE) {
                    readSortedSetFieldWithTable(info, meta);
                } else if (ss.format == Lucene54DocValuesFormat.SORTED_SINGLE_VALUED) {
                    if (meta.readVInt() != fieldNumber) {
                        throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
                    }
                    if (meta.readByte() != Lucene54DocValuesFormat.SORTED) {
                        throw new CorruptIndexException("sortedset entry for field: " + info.name + " is corrupt", meta);
                    }
                    readSortedField(info, meta);
                } else {
                    throw new AssertionError();
                }
            } else if (type == Lucene54DocValuesFormat.SORTED_NUMERIC) {
                SortedSetEntry ss = readSortedSetEntry(meta);
                sortedNumerics.put(info.name, ss);
                if (ss.format == Lucene54DocValuesFormat.SORTED_WITH_ADDRESSES) {
                    if (meta.readVInt() != fieldNumber) {
                        throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
                    }
                    if (meta.readByte() != Lucene54DocValuesFormat.NUMERIC) {
                        throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
                    }
                    numerics.put(info.name, readNumericEntry(info, meta));
                    if (meta.readVInt() != fieldNumber) {
                        throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
                    }
                    if (meta.readByte() != Lucene54DocValuesFormat.NUMERIC) {
                        throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
                    }
                    NumericEntry ordIndex = readNumericEntry(info, meta);
                    ordIndexes.put(info.name, ordIndex);
                } else if (ss.format == Lucene54DocValuesFormat.SORTED_SET_TABLE) {
                    if (meta.readVInt() != info.number) {
                        throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
                    }
                    if (meta.readByte() != Lucene54DocValuesFormat.NUMERIC) {
                        throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
                    }
                    NumericEntry n = readNumericEntry(info, meta);
                    ords.put(info.name, n);
                } else if (ss.format == Lucene54DocValuesFormat.SORTED_SINGLE_VALUED) {
                    if (meta.readVInt() != fieldNumber) {
                        throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
                    }
                    if (meta.readByte() != Lucene54DocValuesFormat.NUMERIC) {
                        throw new CorruptIndexException("sortednumeric entry for field: " + info.name + " is corrupt", meta);
                    }
                    numerics.put(info.name, readNumericEntry(info, meta));
                } else {
                    throw new AssertionError();
                }
            } else {
                throw new CorruptIndexException("invalid type: " + type, meta);
            }
            fieldNumber = meta.readVInt();
        }
        return numFields;
    }

    private NumericEntry readNumericEntry(FieldInfo info, IndexInput meta) throws IOException {
        NumericEntry entry = new NumericEntry();
        entry.format = meta.readVInt();
        entry.missingOffset = meta.readLong();
        if (entry.format == Lucene54DocValuesFormat.SPARSE_COMPRESSED) {
            // sparse bits need a bit more metadata
            entry.numDocsWithValue = meta.readVLong();
            final int blockShift = meta.readVInt();
            entry.monotonicMeta = LegacyDirectMonotonicReader.loadMeta(meta, entry.numDocsWithValue, blockShift);
            ramBytesUsed.addAndGet(entry.monotonicMeta.ramBytesUsed());
            directAddressesMeta.put(info.name, entry.monotonicMeta);
        }
        entry.offset = meta.readLong();
        entry.count = meta.readVLong();
        switch (entry.format) {
            case Lucene54DocValuesFormat.CONST_COMPRESSED:
                entry.minValue = meta.readLong();
                if (entry.count > Integer.MAX_VALUE) {
                    // currently just a limitation e.g. of bits interface and so on.
                    throw new CorruptIndexException("illegal CONST_COMPRESSED count: " + entry.count, meta);
                }
                break;
            case Lucene54DocValuesFormat.GCD_COMPRESSED:
                entry.minValue = meta.readLong();
                entry.gcd = meta.readLong();
                entry.bitsPerValue = meta.readVInt();
                break;
            case Lucene54DocValuesFormat.TABLE_COMPRESSED:
                final int uniqueValues = meta.readVInt();
                if (uniqueValues > 256) {
                    throw new CorruptIndexException(
                        "TABLE_COMPRESSED cannot have more than 256 distinct values, got=" + uniqueValues,
                        meta
                    );
                }
                entry.table = new long[uniqueValues];
                for (int i = 0; i < uniqueValues; ++i) {
                    entry.table[i] = meta.readLong();
                }
                ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(entry.table));
                entry.bitsPerValue = meta.readVInt();
                break;
            case Lucene54DocValuesFormat.DELTA_COMPRESSED:
                entry.minValue = meta.readLong();
                entry.bitsPerValue = meta.readVInt();
                break;
            case Lucene54DocValuesFormat.MONOTONIC_COMPRESSED:
                final int blockShift = meta.readVInt();
                entry.monotonicMeta = LegacyDirectMonotonicReader.loadMeta(meta, maxDoc + 1, blockShift);
                ramBytesUsed.addAndGet(entry.monotonicMeta.ramBytesUsed());
                directAddressesMeta.put(info.name, entry.monotonicMeta);
                break;
            case Lucene54DocValuesFormat.SPARSE_COMPRESSED:
                final byte numberType = meta.readByte();
                switch (numberType) {
                    case 0:
                        entry.numberType = VALUE;
                        break;
                    case 1:
                        entry.numberType = ORDINAL;
                        break;
                    default:
                        throw new CorruptIndexException("Number type can only be 0 or 1, got=" + numberType, meta);
                }

                // now read the numeric entry for non-missing values
                final int fieldNumber = meta.readVInt();
                if (fieldNumber != info.number) {
                    throw new CorruptIndexException("Field numbers mistmatch: " + fieldNumber + " != " + info.number, meta);
                }
                final int dvFormat = meta.readByte();
                if (dvFormat != Lucene54DocValuesFormat.NUMERIC) {
                    throw new CorruptIndexException("Formats mistmatch: " + dvFormat + " != " + Lucene54DocValuesFormat.NUMERIC, meta);
                }
                entry.nonMissingValues = readNumericEntry(info, meta);
                break;
            default:
                throw new CorruptIndexException("Unknown format: " + entry.format + ", input=", meta);
        }
        entry.endOffset = meta.readLong();
        return entry;
    }

    private BinaryEntry readBinaryEntry(FieldInfo info, IndexInput meta) throws IOException {
        BinaryEntry entry = new BinaryEntry();
        entry.format = meta.readVInt();
        entry.missingOffset = meta.readLong();
        entry.minLength = meta.readVInt();
        entry.maxLength = meta.readVInt();
        entry.count = meta.readVLong();
        entry.offset = meta.readLong();
        switch (entry.format) {
            case Lucene54DocValuesFormat.BINARY_FIXED_UNCOMPRESSED:
                break;
            case Lucene54DocValuesFormat.BINARY_PREFIX_COMPRESSED:
                entry.addressesOffset = meta.readLong();
                entry.packedIntsVersion = meta.readVInt();
                entry.blockSize = meta.readVInt();
                entry.reverseIndexOffset = meta.readLong();
                break;
            case Lucene54DocValuesFormat.BINARY_VARIABLE_UNCOMPRESSED:
                entry.addressesOffset = meta.readLong();
                final int blockShift = meta.readVInt();
                entry.addressesMeta = LegacyDirectMonotonicReader.loadMeta(meta, entry.count + 1, blockShift);
                ramBytesUsed.addAndGet(entry.addressesMeta.ramBytesUsed());
                directAddressesMeta.put(info.name, entry.addressesMeta);
                entry.addressesEndOffset = meta.readLong();
                break;
            default:
                throw new CorruptIndexException("Unknown format: " + entry.format, meta);
        }
        return entry;
    }

    SortedSetEntry readSortedSetEntry(IndexInput meta) throws IOException {
        SortedSetEntry entry = new SortedSetEntry();
        entry.format = meta.readVInt();
        if (entry.format == Lucene54DocValuesFormat.SORTED_SET_TABLE) {
            final int totalTableLength = meta.readInt();
            if (totalTableLength > 256) {
                throw new CorruptIndexException(
                    "SORTED_SET_TABLE cannot have more than 256 values in its dictionary, got=" + totalTableLength,
                    meta
                );
            }
            entry.table = new long[totalTableLength];
            for (int i = 0; i < totalTableLength; ++i) {
                entry.table[i] = meta.readLong();
            }
            ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(entry.table));
            final int tableSize = meta.readInt();
            if (tableSize > totalTableLength + 1) { // +1 because of the empty set
                throw new CorruptIndexException(
                    "SORTED_SET_TABLE cannot have more set ids than ords in its dictionary, got "
                        + totalTableLength
                        + " ords and "
                        + tableSize
                        + " sets",
                    meta
                );
            }
            entry.tableOffsets = new int[tableSize + 1];
            for (int i = 1; i < entry.tableOffsets.length; ++i) {
                entry.tableOffsets[i] = entry.tableOffsets[i - 1] + meta.readInt();
            }
            ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(entry.tableOffsets));
        } else if (entry.format != Lucene54DocValuesFormat.SORTED_SINGLE_VALUED
            && entry.format != Lucene54DocValuesFormat.SORTED_WITH_ADDRESSES) {
                throw new CorruptIndexException("Unknown format: " + entry.format, meta);
            }
        return entry;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        NumericEntry entry = numerics.get(field.name);
        Bits docsWithField;

        if (entry.format == Lucene54DocValuesFormat.SPARSE_COMPRESSED) {
            return getSparseNumericDocValues(entry);
        } else {
            if (entry.missingOffset == Lucene54DocValuesFormat.ALL_MISSING) {
                return DocValues.emptyNumeric();
            } else if (entry.missingOffset == Lucene54DocValuesFormat.ALL_LIVE) {
                LongValues values = getNumeric(entry);
                return new NumericDocValues() {
                    private int docID = -1;

                    @Override
                    public int docID() {
                        return docID;
                    }

                    @Override
                    public int nextDoc() {
                        docID++;
                        if (docID == maxDoc) {
                            docID = NO_MORE_DOCS;
                        }
                        return docID;
                    }

                    @Override
                    public int advance(int target) {
                        if (target >= maxDoc) {
                            docID = NO_MORE_DOCS;
                        } else {
                            docID = target;
                        }
                        return docID;
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        docID = target;
                        return true;
                    }

                    @Override
                    public long cost() {
                        // TODO
                        return 0;
                    }

                    @Override
                    public long longValue() {
                        return values.get(docID);
                    }
                };
            } else {
                docsWithField = getLiveBits(entry.missingOffset, maxDoc);
            }
        }
        final LongValues values = getNumeric(entry);
        return new NumericDocValues() {

            int doc = -1;
            long value;

            @Override
            public long longValue() throws IOException {
                return value;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                for (int doc = target; doc < maxDoc; ++doc) {
                    value = values.get(doc);
                    if (value != 0 || docsWithField.get(doc)) {
                        return this.doc = doc;
                    }
                }
                return doc = NO_MORE_DOCS;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                doc = target;
                value = values.get(doc);
                return value != 0 || docsWithField.get(doc);
            }

            @Override
            public long cost() {
                return maxDoc;
            }

        };
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
        switch (entry.format) {
            case Lucene54DocValuesFormat.CONST_COMPRESSED: {
                final long constant = entry.minValue;
                final Bits live = getLiveBits(entry.missingOffset, (int) entry.count);
                return new LongValues() {
                    @Override
                    public long get(long index) {
                        return live.get((int) index) ? constant : 0;
                    }
                };
            }
            case Lucene54DocValuesFormat.DELTA_COMPRESSED: {
                RandomAccessInput slice = this.data.randomAccessSlice(entry.offset, entry.endOffset - entry.offset);
                final long delta = entry.minValue;
                final LongValues values = LegacyDirectReader.getInstance(slice, entry.bitsPerValue, 0);
                return new LongValues() {
                    @Override
                    public long get(long id) {
                        return delta + values.get(id);
                    }
                };
            }
            case Lucene54DocValuesFormat.GCD_COMPRESSED: {
                RandomAccessInput slice = this.data.randomAccessSlice(entry.offset, entry.endOffset - entry.offset);
                final long min = entry.minValue;
                final long mult = entry.gcd;
                final LongValues quotientReader = LegacyDirectReader.getInstance(slice, entry.bitsPerValue, 0);
                return new LongValues() {
                    @Override
                    public long get(long id) {
                        return min + mult * quotientReader.get(id);
                    }
                };
            }
            case Lucene54DocValuesFormat.TABLE_COMPRESSED: {
                RandomAccessInput slice = this.data.randomAccessSlice(entry.offset, entry.endOffset - entry.offset);
                final long table[] = entry.table;
                final LongValues ords = LegacyDirectReader.getInstance(slice, entry.bitsPerValue, 0);
                return new LongValues() {
                    @Override
                    public long get(long id) {
                        return table[(int) ords.get(id)];
                    }
                };
            }
            case Lucene54DocValuesFormat.SPARSE_COMPRESSED:
                final SparseNumericDocValues values = getSparseNumericDocValues(entry);
                final long missingValue;
                switch (entry.numberType) {
                    case ORDINAL:
                        missingValue = -1L;
                        break;
                    case VALUE:
                        missingValue = 0L;
                        break;
                    default:
                        throw new AssertionError();
                }
                return new SparseNumericDocValuesRandomAccessWrapper(values, missingValue);
            default:
                throw new AssertionError();
        }
    }

    static final class SparseNumericDocValues extends NumericDocValues {

        final int docIDsLength;
        final LongValues docIds, values;

        int index, doc;

        SparseNumericDocValues(int docIDsLength, LongValues docIDs, LongValues values) {
            this.docIDsLength = docIDsLength;
            this.docIds = docIDs;
            this.values = values;
            reset();
        }

        void reset() {
            index = -1;
            doc = -1;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            if (index >= docIDsLength - 1) {
                index = docIDsLength;
                return doc = NO_MORE_DOCS;
            }
            return doc = (int) docIds.get(++index);
        }

        @Override
        public int advance(int target) throws IOException {
            long loIndex = index;
            long step = 1;
            long hiIndex;
            int hiDoc;

            // gallop forward by exponentially growing the interval
            // in order to find an interval so that the target doc
            // is in ]lo, hi]. Compared to a regular binary search,
            // this optimizes the case that the caller performs many
            // advance calls by small deltas
            do {
                hiIndex = index + step;
                if (hiIndex >= docIDsLength) {
                    hiIndex = docIDsLength;
                    hiDoc = NO_MORE_DOCS;
                    break;
                }
                hiDoc = (int) docIds.get(hiIndex);
                if (hiDoc >= target) {
                    break;
                }
                step <<= 1;
            } while (true);

            // now binary search
            while (loIndex + 1 < hiIndex) {
                final long midIndex = (loIndex + 1 + hiIndex) >>> 1;
                final int midDoc = (int) docIds.get(midIndex);
                if (midDoc >= target) {
                    hiIndex = midIndex;
                    hiDoc = midDoc;
                } else {
                    loIndex = midIndex;
                }
            }

            index = (int) hiIndex;
            return doc = hiDoc;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (advance(target) == target) {
                return true;
            }
            --index;
            doc = target;
            return index >= 0 && docIds.get(index) == target;
        }

        @Override
        public long longValue() {
            assert index >= 0;
            assert index < docIDsLength;
            return values.get(index);
        }

        @Override
        public long cost() {
            return docIDsLength;
        }
    }

    static class SparseNumericDocValuesRandomAccessWrapper extends LongValues {

        final SparseNumericDocValues values;
        final long missingValue;

        SparseNumericDocValuesRandomAccessWrapper(SparseNumericDocValues values, long missingValue) {
            this.values = values;
            this.missingValue = missingValue;
        }

        @Override
        public long get(long longIndex) {
            final int index = Math.toIntExact(longIndex);
            int doc = values.docID();
            if (doc >= index) {
                values.reset();
            }
            assert values.docID() < index;
            try {
                doc = values.advance(index);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (doc == index) {
                return values.longValue();
            } else {
                return missingValue;
            }
        }

    }

    LegacyBinaryDocValues getLegacyBinary(FieldInfo field) throws IOException {
        BinaryEntry bytes = binaries.get(field.name);
        switch (bytes.format) {
            case Lucene54DocValuesFormat.BINARY_FIXED_UNCOMPRESSED:
                return getFixedBinary(field, bytes);
            case Lucene54DocValuesFormat.BINARY_VARIABLE_UNCOMPRESSED:
                return getVariableBinary(field, bytes);
            case Lucene54DocValuesFormat.BINARY_PREFIX_COMPRESSED:
                return getCompressedBinary(field, bytes);
            default:
                throw new AssertionError();
        }
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        BinaryEntry be = binaries.get(field.name);
        return new LegacyBinaryDocValuesWrapper(getLiveBits(be.missingOffset, maxDoc), getLegacyBinary(field));
    }

    private LegacyBinaryDocValues getFixedBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
        final IndexInput data = this.data.slice("fixed-binary", bytes.offset, bytes.count * bytes.maxLength);

        final BytesRef term = new BytesRef(bytes.maxLength);
        final byte[] buffer = term.bytes;
        final int length = term.length = bytes.maxLength;

        return new LongBinaryDocValues() {
            @Override
            public BytesRef get(long id) {
                try {
                    data.seek(id * length);
                    data.readBytes(buffer, 0, buffer.length);
                    return term;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private LegacyBinaryDocValues getVariableBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
        final RandomAccessInput addressesData = this.data.randomAccessSlice(
            bytes.addressesOffset,
            bytes.addressesEndOffset - bytes.addressesOffset
        );
        final LongValues addresses = LegacyDirectMonotonicReader.getInstance(bytes.addressesMeta, addressesData);

        final IndexInput data = this.data.slice("var-binary", bytes.offset, bytes.addressesOffset - bytes.offset);
        final BytesRef term = new BytesRef(Math.max(0, bytes.maxLength));
        final byte buffer[] = term.bytes;

        return new LongBinaryDocValues() {
            @Override
            public BytesRef get(long id) {
                long startAddress = addresses.get(id);
                long endAddress = addresses.get(id + 1);
                int length = (int) (endAddress - startAddress);
                try {
                    data.seek(startAddress);
                    data.readBytes(buffer, 0, length);
                    term.length = length;
                    return term;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /** returns an address instance for prefix-compressed binary values. */
    private synchronized MonotonicBlockPackedReader getIntervalInstance(FieldInfo field, BinaryEntry bytes) throws IOException {
        MonotonicBlockPackedReader addresses = addressInstances.get(field.name);
        if (addresses == null) {
            data.seek(bytes.addressesOffset);
            final long size = (bytes.count + Lucene54DocValuesFormat.INTERVAL_MASK) >>> Lucene54DocValuesFormat.INTERVAL_SHIFT;
            addresses = MonotonicBlockPackedReader.of(data, bytes.packedIntsVersion, bytes.blockSize, size);
            if (merging == false) {
                addressInstances.put(field.name, addresses);
                ramBytesUsed.addAndGet(addresses.ramBytesUsed() + Integer.BYTES);
            }
        }
        return addresses;
    }

    /** returns a reverse lookup instance for prefix-compressed binary values. */
    private synchronized ReverseTermsIndex getReverseIndexInstance(FieldInfo field, BinaryEntry bytes) throws IOException {
        ReverseTermsIndex index = reverseIndexInstances.get(field.name);
        if (index == null) {
            index = new ReverseTermsIndex();
            data.seek(bytes.reverseIndexOffset);
            long size = (bytes.count + Lucene54DocValuesFormat.REVERSE_INTERVAL_MASK) >>> Lucene54DocValuesFormat.REVERSE_INTERVAL_SHIFT;
            index.termAddresses = MonotonicBlockPackedReader.of(data, bytes.packedIntsVersion, bytes.blockSize, size);
            long dataSize = data.readVLong();
            PagedBytes pagedBytes = new PagedBytes(15);
            pagedBytes.copy(data, dataSize);
            index.terms = pagedBytes.freeze(true);
            if (merging == false) {
                reverseIndexInstances.put(field.name, index);
                ramBytesUsed.addAndGet(index.ramBytesUsed());
            }
        }
        return index;
    }

    private LegacyBinaryDocValues getCompressedBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
        final MonotonicBlockPackedReader addresses = getIntervalInstance(field, bytes);
        final ReverseTermsIndex index = getReverseIndexInstance(field, bytes);
        assert addresses.size() > 0; // we don't have to handle empty case
        IndexInput slice = data.slice("terms", bytes.offset, bytes.addressesOffset - bytes.offset);
        return new CompressedBinaryDocValues(bytes, addresses, index, slice);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        final int valueCount = (int) binaries.get(field.name).count;
        final LegacyBinaryDocValues binary = getLegacyBinary(field);
        NumericEntry entry = ords.get(field.name);
        final LongValues ordinals = getNumeric(entry);
        if (entry.format == Lucene54DocValuesFormat.SPARSE_COMPRESSED) {
            final SparseNumericDocValues sparseValues = ((SparseNumericDocValuesRandomAccessWrapper) ordinals).values;
            return new SortedDocValues() {

                @Override
                public int ordValue() {
                    return (int) sparseValues.longValue();
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
                public int docID() {
                    return sparseValues.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    return sparseValues.nextDoc();
                }

                @Override
                public int advance(int target) throws IOException {
                    return sparseValues.advance(target);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return sparseValues.advanceExact(target);
                }

                @Override
                public long cost() {
                    return sparseValues.cost();
                }

            };
        }
        return new SortedDocValues() {
            private int docID = -1;
            private int ord;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                assert docID != NO_MORE_DOCS;
                while (true) {
                    docID++;
                    if (docID == maxDoc) {
                        docID = NO_MORE_DOCS;
                        break;
                    }
                    ord = (int) ordinals.get(docID);
                    if (ord != -1) {
                        break;
                    }
                }
                return docID;
            }

            @Override
            public int advance(int target) throws IOException {
                if (target >= maxDoc) {
                    docID = NO_MORE_DOCS;
                    return docID;
                } else {
                    docID = target - 1;
                    return nextDoc();
                }
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                docID = target;
                ord = (int) ordinals.get(target);
                return ord != -1;
            }

            @Override
            public int ordValue() {
                return ord;
            }

            @Override
            public long cost() {
                // TODO
                return 0;
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
            public int lookupTerm(BytesRef key) throws IOException {
                if (binary instanceof CompressedBinaryDocValues) {
                    return (int) ((CompressedBinaryDocValues) binary).lookupTerm(key);
                } else {
                    return super.lookupTerm(key);
                }
            }

            @Override
            public TermsEnum termsEnum() throws IOException {
                if (binary instanceof CompressedBinaryDocValues) {
                    return ((CompressedBinaryDocValues) binary).getTermsEnum();
                } else {
                    return super.termsEnum();
                }
            }
        };
    }

    /** returns an address instance for sortedset ordinal lists */
    private LongValues getOrdIndexInstance(FieldInfo field, NumericEntry entry) throws IOException {
        RandomAccessInput data = this.data.randomAccessSlice(entry.offset, entry.endOffset - entry.offset);
        return LegacyDirectMonotonicReader.getInstance(entry.monotonicMeta, data);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        SortedSetEntry ss = sortedNumerics.get(field.name);
        if (ss.format == Lucene54DocValuesFormat.SORTED_SINGLE_VALUED) {
            NumericEntry numericEntry = numerics.get(field.name);
            final LongValues values = getNumeric(numericEntry);
            if (numericEntry.format == Lucene54DocValuesFormat.SPARSE_COMPRESSED) {
                SparseNumericDocValues sparseValues = ((SparseNumericDocValuesRandomAccessWrapper) values).values;
                return new SortedNumericDocValues() {

                    @Override
                    public long nextValue() throws IOException {
                        return sparseValues.longValue();
                    }

                    @Override
                    public int docValueCount() {
                        return 1;
                    }

                    @Override
                    public int docID() {
                        return sparseValues.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        return sparseValues.nextDoc();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        return sparseValues.advance(target);
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return sparseValues.advanceExact(target);
                    }

                    @Override
                    public long cost() {
                        return sparseValues.cost();
                    }

                };
            }
            final Bits docsWithField = getLiveBits(numericEntry.missingOffset, maxDoc);
            return new SortedNumericDocValues() {
                int docID = -1;

                @Override
                public int docID() {
                    return docID;
                }

                @Override
                public int nextDoc() {
                    while (true) {
                        docID++;
                        if (docID == maxDoc) {
                            docID = NO_MORE_DOCS;
                            break;
                        }

                        if (docsWithField.get(docID)) {
                            // TODO: use .nextSetBit here, at least!!
                            break;
                        }
                    }
                    return docID;
                }

                @Override
                public int advance(int target) {
                    if (target >= maxDoc) {
                        docID = NO_MORE_DOCS;
                        return docID;
                    } else {
                        docID = target - 1;
                        return nextDoc();
                    }
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    docID = target;
                    return docsWithField.get(docID);
                }

                @Override
                public long cost() {
                    // TODO
                    return 0;
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                @Override
                public long nextValue() {
                    return values.get(docID);
                }
            };
        } else if (ss.format == Lucene54DocValuesFormat.SORTED_WITH_ADDRESSES) {
            NumericEntry numericEntry = numerics.get(field.name);
            final LongValues values = getNumeric(numericEntry);
            final LongValues ordIndex = getOrdIndexInstance(field, ordIndexes.get(field.name));

            return new SortedNumericDocValues() {
                long startOffset;
                long endOffset;
                int docID = -1;
                long upto;

                @Override
                public int docID() {
                    return docID;
                }

                @Override
                public int nextDoc() {
                    while (true) {
                        docID++;
                        if (docID == maxDoc) {
                            docID = NO_MORE_DOCS;
                            return docID;
                        }
                        startOffset = ordIndex.get(docID);
                        endOffset = ordIndex.get(docID + 1L);
                        if (endOffset > startOffset) {
                            break;
                        }
                    }
                    upto = startOffset;
                    return docID;
                }

                @Override
                public int advance(int target) {
                    if (target >= maxDoc) {
                        docID = NO_MORE_DOCS;
                        return docID;
                    } else {
                        docID = target - 1;
                        return nextDoc();
                    }
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    docID = target;
                    startOffset = ordIndex.get(docID);
                    endOffset = ordIndex.get(docID + 1L);
                    upto = startOffset;
                    return endOffset > startOffset;
                }

                @Override
                public long cost() {
                    // TODO
                    return 0;
                }

                @Override
                public int docValueCount() {
                    return (int) (endOffset - startOffset);
                }

                @Override
                public long nextValue() {
                    return values.get(upto++);
                }
            };
        } else if (ss.format == Lucene54DocValuesFormat.SORTED_SET_TABLE) {
            NumericEntry entry = ords.get(field.name);
            final LongValues ordinals = getNumeric(entry);

            final long[] table = ss.table;
            final int[] offsets = ss.tableOffsets;
            return new SortedNumericDocValues() {
                int startOffset;
                int endOffset;
                int docID = -1;
                int upto;

                @Override
                public int docID() {
                    return docID;
                }

                @Override
                public int nextDoc() {
                    while (true) {
                        docID++;
                        if (docID == maxDoc) {
                            docID = NO_MORE_DOCS;
                            return docID;
                        }
                        int ord = (int) ordinals.get(docID);
                        startOffset = offsets[ord];
                        endOffset = offsets[ord + 1];
                        if (endOffset > startOffset) {
                            break;
                        }
                    }
                    upto = startOffset;
                    return docID;
                }

                @Override
                public int advance(int target) {
                    if (target >= maxDoc) {
                        docID = NO_MORE_DOCS;
                        return docID;
                    } else {
                        docID = target - 1;
                        return nextDoc();
                    }
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    docID = target;
                    int ord = (int) ordinals.get(docID);
                    startOffset = offsets[ord];
                    endOffset = offsets[ord + 1];
                    upto = startOffset;
                    return endOffset > startOffset;
                }

                @Override
                public long cost() {
                    // TODO
                    return 0;
                }

                @Override
                public int docValueCount() {
                    return endOffset - startOffset;
                }

                @Override
                public long nextValue() {
                    return table[upto++];
                }
            };
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        SortedSetEntry ss = sortedSets.get(field.name);
        switch (ss.format) {
            case Lucene54DocValuesFormat.SORTED_SINGLE_VALUED:
                return DocValues.singleton(getSorted(field));
            case Lucene54DocValuesFormat.SORTED_WITH_ADDRESSES:
                return getSortedSetWithAddresses(field);
            case Lucene54DocValuesFormat.SORTED_SET_TABLE:
                return getSortedSetTable(field, ss);
            default:
                throw new AssertionError();
        }
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return null;
    }

    private SortedSetDocValues getSortedSetWithAddresses(FieldInfo field) throws IOException {
        final long valueCount = binaries.get(field.name).count;
        // we keep the byte[]s and list of ords on disk, these could be large
        final LongBinaryDocValues binary = (LongBinaryDocValues) getLegacyBinary(field);
        final LongValues ordinals = getNumeric(ords.get(field.name));
        // but the addresses to the ord stream are in RAM
        final LongValues ordIndex = getOrdIndexInstance(field, ordIndexes.get(field.name));

        return new LegacySortedSetDocValuesWrapper(new LegacySortedSetDocValues() {
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
                endOffset = ordIndex.get(docID + 1L);
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return binary.get(ord);
            }

            @Override
            public int docValueCount() {
                return Math.toIntExact(endOffset - startOffset);
            }

            @Override
            public long getValueCount() {
                return valueCount;
            }

            @Override
            public long lookupTerm(BytesRef key) {
                if (binary instanceof CompressedBinaryDocValues) {
                    return ((CompressedBinaryDocValues) binary).lookupTerm(key);
                } else {
                    return super.lookupTerm(key);
                }
            }

            @Override
            public TermsEnum termsEnum() throws IOException {
                if (binary instanceof CompressedBinaryDocValues) {
                    return ((CompressedBinaryDocValues) binary).getTermsEnum();
                } else {
                    return super.termsEnum();
                }
            }
        }, maxDoc);
    }

    private SortedSetDocValues getSortedSetTable(FieldInfo field, SortedSetEntry ss) throws IOException {
        final long valueCount = binaries.get(field.name).count;
        final LongBinaryDocValues binary = (LongBinaryDocValues) getLegacyBinary(field);
        final NumericEntry ordinalsEntry = ords.get(field.name);
        final LongValues ordinals = getNumeric(ordinalsEntry);

        final long[] table = ss.table;
        final int[] offsets = ss.tableOffsets;

        return new LegacySortedSetDocValuesWrapper(new LegacySortedSetDocValues() {

            int offset, startOffset, endOffset;

            @Override
            public void setDocument(int docID) {
                final int ord = (int) ordinals.get(docID);
                offset = startOffset = offsets[ord];
                endOffset = offsets[ord + 1];
            }

            @Override
            public long nextOrd() {
                if (offset == endOffset) {
                    return NO_MORE_ORDS;
                } else {
                    return table[offset++];
                }
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
            public int docValueCount() {
                return Math.toIntExact(endOffset - startOffset);
            }

            @Override
            public long lookupTerm(BytesRef key) {
                if (binary instanceof CompressedBinaryDocValues) {
                    return ((CompressedBinaryDocValues) binary).lookupTerm(key);
                } else {
                    return super.lookupTerm(key);
                }
            }

            @Override
            public TermsEnum termsEnum() throws IOException {
                if (binary instanceof CompressedBinaryDocValues) {
                    return ((CompressedBinaryDocValues) binary).getTermsEnum();
                } else {
                    return super.termsEnum();
                }
            }
        }, maxDoc);
    }

    private Bits getLiveBits(final long offset, final int count) throws IOException {
        if (offset == Lucene54DocValuesFormat.ALL_MISSING) {
            return new Bits.MatchNoBits(count);
        } else if (offset == Lucene54DocValuesFormat.ALL_LIVE) {
            return new Bits.MatchAllBits(count);
        } else {
            int length = (int) ((count + 7L) >>> 3);
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
                    return count;
                }
            };
        }
    }

    private SparseNumericDocValues getSparseNumericDocValues(NumericEntry entry) throws IOException {
        final RandomAccessInput docIdsData = this.data.randomAccessSlice(entry.missingOffset, entry.offset - entry.missingOffset);
        final LongValues docIDs = LegacyDirectMonotonicReader.getInstance(entry.monotonicMeta, docIdsData);
        final LongValues values = getNumeric(entry.nonMissingValues); // cannot be sparse
        return new SparseNumericDocValues(Math.toIntExact(entry.numDocsWithValue), docIDs, values);
    }

    @Override
    public synchronized DocValuesProducer getMergeInstance() {
        return new Lucene54DocValuesProducer(this);
    }

    @Override
    public void close() throws IOException {
        data.close();
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
        /** count of values written */
        public long count;

        /** monotonic meta */
        public LegacyDirectMonotonicReader.Meta monotonicMeta;

        long minValue;
        long gcd;
        long table[];

        /** for sparse compression */
        long numDocsWithValue;
        NumericEntry nonMissingValues;
        Lucene54DocValuesConsumer.NumberType numberType;
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
        public long addressesOffset, addressesEndOffset;
        /** meta data for addresses */
        public LegacyDirectMonotonicReader.Meta addressesMeta;
        /** offset to the reverse index */
        public long reverseIndexOffset;
        /** packed ints version used to encode addressing information */
        public int packedIntsVersion;
        /** packed ints blocksize */
        public int blockSize;
    }

    /** metadata entry for a sorted-set docvalues field */
    static class SortedSetEntry {
        private SortedSetEntry() {}

        int format;

        long[] table;
        int[] tableOffsets;
    }

    // internally we compose complex dv (sorted/sortedset) from other ones
    abstract static class LongBinaryDocValues extends LegacyBinaryDocValues {
        @Override
        public final BytesRef get(int docID) {
            return get((long) docID);
        }

        abstract BytesRef get(long id);
    }

    // used for reverse lookup to a small range of blocks
    static class ReverseTermsIndex implements Accountable {
        public MonotonicBlockPackedReader termAddresses;
        public PagedBytes.Reader terms;

        @Override
        public long ramBytesUsed() {
            return termAddresses.ramBytesUsed() + terms.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            List<Accountable> resources = new ArrayList<>();
            resources.add(Accountables.namedAccountable("term bytes", terms));
            resources.add(Accountables.namedAccountable("term addresses", termAddresses));
            return Collections.unmodifiableList(resources);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(size=" + termAddresses.size() + ")";
        }
    }

    // in the compressed case, we add a few additional operations for
    // more efficient reverse lookup and enumeration
    static final class CompressedBinaryDocValues extends LongBinaryDocValues {
        final long numValues;
        final long numIndexValues;
        final int maxTermLength;
        final MonotonicBlockPackedReader addresses;
        final IndexInput data;
        final CompressedBinaryTermsEnum termsEnum;
        final PagedBytes.Reader reverseTerms;
        final MonotonicBlockPackedReader reverseAddresses;
        final long numReverseIndexValues;

        CompressedBinaryDocValues(BinaryEntry bytes, MonotonicBlockPackedReader addresses, ReverseTermsIndex index, IndexInput data)
            throws IOException {
            this.maxTermLength = bytes.maxLength;
            this.numValues = bytes.count;
            this.addresses = addresses;
            this.numIndexValues = addresses.size();
            this.data = data;
            this.reverseTerms = index.terms;
            this.reverseAddresses = index.termAddresses;
            this.numReverseIndexValues = reverseAddresses.size();
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
                switch (termsEnum.seekCeil(key)) {
                    case FOUND:
                        return termsEnum.ord();
                    case NOT_FOUND:
                        return -termsEnum.ord() - 1;
                    default:
                        return -numValues - 1;
                }
            } catch (IOException bogus) {
                throw new RuntimeException(bogus);
            }
        }

        TermsEnum getTermsEnum() throws IOException {
            return getTermsEnum(data.clone());
        }

        private CompressedBinaryTermsEnum getTermsEnum(IndexInput input) throws IOException {
            return new CompressedBinaryTermsEnum(input);
        }

        class CompressedBinaryTermsEnum extends BaseTermsEnum {
            private long currentOrd = -1;
            // offset to the start of the current block
            private long currentBlockStart;
            private final IndexInput input;
            // delta from currentBlockStart to start of each term
            private final int offsets[] = new int[Lucene54DocValuesFormat.INTERVAL_COUNT];
            private final byte buffer[] = new byte[2 * Lucene54DocValuesFormat.INTERVAL_COUNT - 1];

            private final BytesRef term = new BytesRef(maxTermLength);
            private final BytesRef firstTerm = new BytesRef(maxTermLength);
            private final BytesRef scratch = new BytesRef();

            CompressedBinaryTermsEnum(IndexInput input) throws IOException {
                this.input = input;
                input.seek(0);
            }

            private void readHeader() throws IOException {
                firstTerm.length = input.readVInt();
                input.readBytes(firstTerm.bytes, 0, firstTerm.length);
                input.readBytes(buffer, 0, Lucene54DocValuesFormat.INTERVAL_COUNT - 1);
                if (buffer[0] == -1) {
                    readShortAddresses();
                } else {
                    readByteAddresses();
                }
                currentBlockStart = input.getFilePointer();
            }

            // read single byte addresses: each is delta - 2
            // (shared prefix byte and length > 0 are both implicit)
            private void readByteAddresses() throws IOException {
                int addr = 0;
                for (int i = 1; i < offsets.length; i++) {
                    addr += 2 + (buffer[i - 1] & 0xFF);
                    offsets[i] = addr;
                }
            }

            // read double byte addresses: each is delta - 2
            // (shared prefix byte and length > 0 are both implicit)
            private void readShortAddresses() throws IOException {
                input.readBytes(buffer, Lucene54DocValuesFormat.INTERVAL_COUNT - 1, Lucene54DocValuesFormat.INTERVAL_COUNT);
                int addr = 0;
                for (int i = 1; i < offsets.length; i++) {
                    int x = i << 1;
                    addr += 2 + ((buffer[x - 1] << 8) | (buffer[x] & 0xFF));
                    offsets[i] = addr;
                }
            }

            // set term to the first term
            private void readFirstTerm() throws IOException {
                term.length = firstTerm.length;
                System.arraycopy(firstTerm.bytes, firstTerm.offset, term.bytes, 0, term.length);
            }

            // read term at offset, delta encoded from first term
            private void readTerm(int offset) throws IOException {
                int start = input.readByte() & 0xFF;
                System.arraycopy(firstTerm.bytes, firstTerm.offset, term.bytes, 0, start);
                int suffix = offsets[offset] - offsets[offset - 1] - 1;
                input.readBytes(term.bytes, start, suffix);
                term.length = start + suffix;
            }

            @Override
            public BytesRef next() throws IOException {
                currentOrd++;
                if (currentOrd >= numValues) {
                    return null;
                } else {
                    int offset = (int) (currentOrd & Lucene54DocValuesFormat.INTERVAL_MASK);
                    if (offset == 0) {
                        // switch to next block
                        readHeader();
                        readFirstTerm();
                    } else {
                        readTerm(offset);
                    }
                    return term;
                }
            }

            // binary search reverse index to find smaller
            // range of blocks to search
            long binarySearchIndex(BytesRef text) throws IOException {
                long low = 0;
                long high = numReverseIndexValues - 1;
                while (low <= high) {
                    long mid = (low + high) >>> 1;
                    reverseTerms.fill(scratch, reverseAddresses.get(mid));
                    int cmp = scratch.compareTo(text);

                    if (cmp < 0) {
                        low = mid + 1;
                    } else if (cmp > 0) {
                        high = mid - 1;
                    } else {
                        return mid;
                    }
                }
                return high;
            }

            // binary search against first term in block range
            // to find term's block
            long binarySearchBlock(BytesRef text, long low, long high) throws IOException {
                while (low <= high) {
                    long mid = (low + high) >>> 1;
                    input.seek(addresses.get(mid));
                    term.length = input.readVInt();
                    input.readBytes(term.bytes, 0, term.length);
                    int cmp = term.compareTo(text);

                    if (cmp < 0) {
                        low = mid + 1;
                    } else if (cmp > 0) {
                        high = mid - 1;
                    } else {
                        return mid;
                    }
                }
                return high;
            }

            @Override
            public SeekStatus seekCeil(BytesRef text) throws IOException {
                // locate block: narrow to block range with index, then search blocks
                final long block;
                long indexPos = binarySearchIndex(text);
                if (indexPos < 0) {
                    block = 0;
                } else {
                    long low = indexPos << Lucene54DocValuesFormat.BLOCK_INTERVAL_SHIFT;
                    long high = Math.min(numIndexValues - 1, low + Lucene54DocValuesFormat.BLOCK_INTERVAL_MASK);
                    block = Math.max(low, binarySearchBlock(text, low, high));
                }

                // position before block, then scan to term.
                input.seek(addresses.get(block));
                currentOrd = (block << Lucene54DocValuesFormat.INTERVAL_SHIFT) - 1;

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
                long block = ord >>> Lucene54DocValuesFormat.INTERVAL_SHIFT;
                if (block != currentOrd >>> Lucene54DocValuesFormat.INTERVAL_SHIFT) {
                    // switch to different block
                    input.seek(addresses.get(block));
                    readHeader();
                }

                currentOrd = ord;

                int offset = (int) (ord & Lucene54DocValuesFormat.INTERVAL_MASK);
                if (offset == 0) {
                    readFirstTerm();
                } else {
                    input.seek(currentBlockStart + offsets[offset - 1]);
                    readTerm(offset);
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

            @Override
            public ImpactsEnum impacts(int flags) throws IOException {
                throw new UnsupportedOperationException();
            }

        }
    }
}
