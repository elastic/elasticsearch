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

import org.apache.lucene.backward_codecs.packed.LegacyDirectMonotonicWriter;
import org.apache.lucene.backward_codecs.packed.LegacyDirectWriter;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.lucene.bwc.codecs.index.LegacyDocValuesIterables;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.StreamSupport;

import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.ALL_LIVE;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.ALL_MISSING;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.BINARY_FIXED_UNCOMPRESSED;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.BINARY_PREFIX_COMPRESSED;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.BINARY_VARIABLE_UNCOMPRESSED;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.CONST_COMPRESSED;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.DELTA_COMPRESSED;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.GCD_COMPRESSED;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.INTERVAL_COUNT;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.INTERVAL_MASK;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.INTERVAL_SHIFT;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.MONOTONIC_BLOCK_SIZE;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.MONOTONIC_COMPRESSED;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.REVERSE_INTERVAL_COUNT;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.REVERSE_INTERVAL_MASK;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.SORTED_SET_TABLE;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.SORTED_SINGLE_VALUED;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.SORTED_WITH_ADDRESSES;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.SPARSE_COMPRESSED;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat.TABLE_COMPRESSED;

/** writer for {@link Lucene54DocValuesFormat} */
final class Lucene54DocValuesConsumer extends DocValuesConsumer implements Closeable {

    enum NumberType {
        /** Dense ordinals */
        ORDINAL,
        /** Random long values */
        VALUE;
    }

    IndexOutput data, meta;
    final int maxDoc;

    /** expert: Creates a new writer */
    Lucene54DocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = EndiannessReverserUtil.createOutput(state.directory, dataName, state.context);
            CodecUtil.writeIndexHeader(
                data,
                dataCodec,
                Lucene54DocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = EndiannessReverserUtil.createOutput(state.directory, metaName, state.context);
            CodecUtil.writeIndexHeader(
                meta,
                metaCodec,
                Lucene54DocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            maxDoc = state.segmentInfo.maxDoc();
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        addNumericField(field, LegacyDocValuesIterables.numericIterable(field, valuesProducer, maxDoc), NumberType.VALUE);
    }

    void addNumericField(FieldInfo field, Iterable<Number> values, NumberType numberType) throws IOException {
        long count = 0;
        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;
        long gcd = 0;
        long missingCount = 0;
        long zeroCount = 0;
        // TODO: more efficient?
        HashSet<Long> uniqueValues = null;
        long missingOrdCount = 0;
        if (numberType == NumberType.VALUE) {
            uniqueValues = new HashSet<>();

            for (Number nv : values) {
                final long v;
                if (nv == null) {
                    v = 0;
                    missingCount++;
                    zeroCount++;
                } else {
                    v = nv.longValue();
                    if (v == 0) {
                        zeroCount++;
                    }
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
            for (Number nv : values) {
                long v = nv.longValue();
                if (v == -1L) {
                    missingOrdCount++;
                }
                minValue = Math.min(minValue, v);
                maxValue = Math.max(maxValue, v);
                ++count;
            }
        }

        final long delta = maxValue - minValue;
        final int deltaBitsRequired = LegacyDirectWriter.unsignedBitsRequired(delta);
        final int tableBitsRequired = uniqueValues == null ? Integer.MAX_VALUE : LegacyDirectWriter.bitsRequired(uniqueValues.size() - 1);

        final boolean sparse; // 1% of docs or less have a value
        switch (numberType) {
            case VALUE:
                sparse = (double) missingCount / count >= 0.99;
                break;
            case ORDINAL:
                sparse = (double) missingOrdCount / count >= 0.99;
                break;
            default:
                throw new AssertionError();
        }

        final int format;
        if (uniqueValues != null
            && count <= Integer.MAX_VALUE
            && (uniqueValues.size() == 1 || (uniqueValues.size() == 2 && missingCount > 0 && zeroCount == missingCount))) {
            // either one unique value C or two unique values: "missing" and C
            format = CONST_COMPRESSED;
        } else if (sparse && count >= 1024) {
            // require at least 1024 docs to avoid flipping back and forth when doing NRT search
            format = SPARSE_COMPRESSED;
        } else if (uniqueValues != null && tableBitsRequired < deltaBitsRequired) {
            format = TABLE_COMPRESSED;
        } else if (gcd != 0 && gcd != 1) {
            final long gcdDelta = (maxValue - minValue) / gcd;
            final long gcdBitsRequired = LegacyDirectWriter.unsignedBitsRequired(gcdDelta);
            format = gcdBitsRequired < deltaBitsRequired ? GCD_COMPRESSED : DELTA_COMPRESSED;
        } else {
            format = DELTA_COMPRESSED;
        }
        meta.writeVInt(field.number);
        meta.writeByte(Lucene54DocValuesFormat.NUMERIC);
        meta.writeVInt(format);
        if (format == SPARSE_COMPRESSED) {
            meta.writeLong(data.getFilePointer());
            final long numDocsWithValue;
            switch (numberType) {
                case VALUE:
                    numDocsWithValue = count - missingCount;
                    break;
                case ORDINAL:
                    numDocsWithValue = count - missingOrdCount;
                    break;
                default:
                    throw new AssertionError();
            }
            final long maxDoc = writeSparseMissingBitset(values, numberType, numDocsWithValue);
            assert maxDoc == count;
        } else if (missingCount == 0) {
            meta.writeLong(ALL_LIVE);
        } else if (missingCount == count) {
            meta.writeLong(ALL_MISSING);
        } else {
            meta.writeLong(data.getFilePointer());
            writeMissingBitset(values);
        }
        meta.writeLong(data.getFilePointer());
        meta.writeVLong(count);

        switch (format) {
            case CONST_COMPRESSED:
                // write the constant (nonzero value in the n=2 case, singleton value otherwise)
                meta.writeLong(minValue < 0 ? Collections.min(uniqueValues) : Collections.max(uniqueValues));
                break;
            case GCD_COMPRESSED:
                meta.writeLong(minValue);
                meta.writeLong(gcd);
                final long maxDelta = (maxValue - minValue) / gcd;
                final int bits = LegacyDirectWriter.unsignedBitsRequired(maxDelta);
                meta.writeVInt(bits);
                final LegacyDirectWriter quotientWriter = LegacyDirectWriter.getInstance(data, count, bits);
                for (Number nv : values) {
                    long value = nv == null ? 0 : nv.longValue();
                    quotientWriter.add((value - minValue) / gcd);
                }
                quotientWriter.finish();
                break;
            case DELTA_COMPRESSED:
                final long minDelta = delta < 0 ? 0 : minValue;
                meta.writeLong(minDelta);
                meta.writeVInt(deltaBitsRequired);
                final LegacyDirectWriter writer = LegacyDirectWriter.getInstance(data, count, deltaBitsRequired);
                for (Number nv : values) {
                    long v = nv == null ? 0 : nv.longValue();
                    writer.add(v - minDelta);
                }
                writer.finish();
                break;
            case TABLE_COMPRESSED:
                final Long[] decode = uniqueValues.toArray(new Long[uniqueValues.size()]);
                Arrays.sort(decode);
                final HashMap<Long, Integer> encode = new HashMap<>();
                meta.writeVInt(decode.length);
                for (int i = 0; i < decode.length; i++) {
                    meta.writeLong(decode[i]);
                    encode.put(decode[i], i);
                }
                meta.writeVInt(tableBitsRequired);
                final LegacyDirectWriter ordsWriter = LegacyDirectWriter.getInstance(data, count, tableBitsRequired);
                for (Number nv : values) {
                    ordsWriter.add(encode.get(nv == null ? 0 : nv.longValue()));
                }
                ordsWriter.finish();
                break;
            case SPARSE_COMPRESSED:
                final Iterable<Number> filteredMissingValues;
                switch (numberType) {
                    case VALUE:
                        meta.writeByte((byte) 0);
                        filteredMissingValues = new Iterable<Number>() {
                            @Override
                            public Iterator<Number> iterator() {
                                return StreamSupport.stream(values.spliterator(), false).filter(value -> value != null).iterator();
                            }
                        };
                        break;
                    case ORDINAL:
                        meta.writeByte((byte) 1);
                        filteredMissingValues = new Iterable<Number>() {
                            @Override
                            public Iterator<Number> iterator() {
                                return StreamSupport.stream(values.spliterator(), false)
                                    .filter(value -> value.longValue() != -1L)
                                    .iterator();
                            }
                        };
                        break;
                    default:
                        throw new AssertionError();
                }
                // Write non-missing values as a numeric field
                addNumericField(field, filteredMissingValues, numberType);
                break;
            default:
                throw new AssertionError();
        }
        meta.writeLong(data.getFilePointer());
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
                bits |= (byte) (1 << (count & 7));
            }
            count++;
        }
        if (count > 0) {
            data.writeByte(bits);
        }
    }

    long writeSparseMissingBitset(Iterable<Number> values, NumberType numberType, long numDocsWithValue) throws IOException {
        meta.writeVLong(numDocsWithValue);

        // Write doc IDs that have a value
        meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
        final LegacyDirectMonotonicWriter docIdsWriter = LegacyDirectMonotonicWriter.getInstance(
            meta,
            data,
            numDocsWithValue,
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );
        long docID = 0;
        for (Number nv : values) {
            switch (numberType) {
                case VALUE:
                    if (nv != null) {
                        docIdsWriter.add(docID);
                    }
                    break;
                case ORDINAL:
                    if (nv.longValue() != -1L) {
                        docIdsWriter.add(docID);
                    }
                    break;
                default:
                    throw new AssertionError();
            }
            docID++;
        }
        docIdsWriter.finish();
        return docID;
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        addBinaryField(field, LegacyDocValuesIterables.binaryIterable(field, valuesProducer, maxDoc));
    }

    private void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
        // write the byte[] data
        meta.writeVInt(field.number);
        meta.writeByte(Lucene54DocValuesFormat.BINARY);
        int minLength = Integer.MAX_VALUE;
        int maxLength = Integer.MIN_VALUE;
        final long startFP = data.getFilePointer();
        long count = 0;
        long missingCount = 0;
        for (BytesRef v : values) {
            final int length;
            if (v == null) {
                length = 0;
                missingCount++;
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
        if (missingCount == 0) {
            meta.writeLong(ALL_LIVE);
        } else if (missingCount == count) {
            meta.writeLong(ALL_MISSING);
        } else {
            meta.writeLong(data.getFilePointer());
            writeMissingBitset(values);
        }
        meta.writeVInt(minLength);
        meta.writeVInt(maxLength);
        meta.writeVLong(count);
        meta.writeLong(startFP);

        // if minLength == maxLength, it's a fixed-length byte[], we are done (the addresses are implicit)
        // otherwise, we need to record the length fields...
        if (minLength != maxLength) {
            meta.writeLong(data.getFilePointer());
            meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

            final LegacyDirectMonotonicWriter writer = LegacyDirectMonotonicWriter.getInstance(
                meta,
                data,
                count + 1,
                DIRECT_MONOTONIC_BLOCK_SHIFT
            );
            long addr = 0;
            writer.add(addr);
            for (BytesRef v : values) {
                if (v != null) {
                    addr += v.length;
                }
                writer.add(addr);
            }
            writer.finish();
            meta.writeLong(data.getFilePointer());
        }
    }

    /** expert: writes a value dictionary for a sorted/sortedset field */
    private void addTermsDict(FieldInfo field, final Iterable<BytesRef> values) throws IOException {
        // first check if it's a "fixed-length" terms dict, and compressibility if so
        int minLength = Integer.MAX_VALUE;
        int maxLength = Integer.MIN_VALUE;
        long numValues = 0;
        BytesRefBuilder previousValue = new BytesRefBuilder();
        long prefixSum = 0; // only valid for fixed-width data, as we have a choice there
        for (BytesRef v : values) {
            minLength = Math.min(minLength, v.length);
            maxLength = Math.max(maxLength, v.length);
            if (minLength == maxLength) {
                int termPosition = (int) (numValues & INTERVAL_MASK);
                if (termPosition == 0) {
                    // first term in block, save it away to compare against the last term later
                    previousValue.copyBytes(v);
                } else if (termPosition == INTERVAL_COUNT - 1) {
                    // last term in block, accumulate shared prefix against first term
                    prefixSum += LegacyStringHelper.bytesDifference(previousValue.get(), v);
                }
            }
            numValues++;
        }
        // for fixed width data, look at the avg(shared prefix) before deciding how to encode:
        // prefix compression "costs" worst case 2 bytes per term because we must store suffix lengths.
        // so if we share at least 3 bytes on average, always compress.
        if (minLength == maxLength && prefixSum <= 3 * (numValues >> INTERVAL_SHIFT)) {
            // no index needed: not very compressible, direct addressing by mult
            addBinaryField(field, values);
        } else if (numValues < REVERSE_INTERVAL_COUNT) {
            // low cardinality: waste a few KB of ram, but can't really use fancy index etc
            addBinaryField(field, values);
        } else {
            assert numValues > 0; // we don't have to handle the empty case
            // header
            meta.writeVInt(field.number);
            meta.writeByte(Lucene54DocValuesFormat.BINARY);
            meta.writeVInt(BINARY_PREFIX_COMPRESSED);
            meta.writeLong(-1L);
            // now write the bytes: sharing prefixes within a block
            final long startFP = data.getFilePointer();
            // currently, we have to store the delta from expected for every 1/nth term
            // we could avoid this, but it's not much and less overall RAM than the previous approach!
            ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
            MonotonicBlockPackedWriter termAddresses = new MonotonicBlockPackedWriter(addressBuffer, MONOTONIC_BLOCK_SIZE);
            // buffers up 16 terms
            ByteBuffersDataOutput bytesBuffer = new ByteBuffersDataOutput();
            // buffers up block header
            ByteBuffersDataOutput headerBuffer = new ByteBuffersDataOutput();
            BytesRefBuilder lastTerm = new BytesRefBuilder();
            lastTerm.grow(maxLength);
            long count = 0;
            int suffixDeltas[] = new int[INTERVAL_COUNT];
            for (BytesRef v : values) {
                int termPosition = (int) (count & INTERVAL_MASK);
                if (termPosition == 0) {
                    termAddresses.add(data.getFilePointer() - startFP);
                    // abs-encode first term
                    headerBuffer.writeVInt(v.length);
                    headerBuffer.writeBytes(v.bytes, v.offset, v.length);
                    lastTerm.copyBytes(v);
                } else {
                    // prefix-code: we only share at most 255 characters, to encode the length as a single
                    // byte and have random access. Larger terms just get less compression.
                    int sharedPrefix = Math.min(255, LegacyStringHelper.bytesDifference(lastTerm.get(), v));
                    bytesBuffer.writeByte((byte) sharedPrefix);
                    bytesBuffer.writeBytes(v.bytes, v.offset + sharedPrefix, v.length - sharedPrefix);
                    // we can encode one smaller, because terms are unique.
                    suffixDeltas[termPosition] = v.length - sharedPrefix - 1;
                }

                count++;
                // flush block
                if ((count & INTERVAL_MASK) == 0) {
                    flushTermsDictBlock(headerBuffer, bytesBuffer, suffixDeltas);
                }
            }
            // flush trailing crap
            int leftover = (int) (count & INTERVAL_MASK);
            if (leftover > 0) {
                Arrays.fill(suffixDeltas, leftover, suffixDeltas.length, 0);
                flushTermsDictBlock(headerBuffer, bytesBuffer, suffixDeltas);
            }
            final long indexStartFP = data.getFilePointer();
            // write addresses of indexed terms
            termAddresses.finish();
            addressBuffer.copyTo(data);
            addressBuffer = null;
            termAddresses = null;
            meta.writeVInt(minLength);
            meta.writeVInt(maxLength);
            meta.writeVLong(count);
            meta.writeLong(startFP);
            meta.writeLong(indexStartFP);
            meta.writeVInt(PackedInts.VERSION_MONOTONIC_WITHOUT_ZIGZAG);
            meta.writeVInt(MONOTONIC_BLOCK_SIZE);
            addReverseTermIndex(field, values, maxLength);
        }
    }

    // writes term dictionary "block"
    // first term is absolute encoded as vint length + bytes.
    // lengths of subsequent N terms are encoded as either N bytes or N shorts.
    // in the double-byte case, the first byte is indicated with -1.
    // subsequent terms are encoded as byte suffixLength + bytes.
    private void flushTermsDictBlock(ByteBuffersDataOutput headerBuffer, ByteBuffersDataOutput bytesBuffer, int suffixDeltas[])
        throws IOException {
        boolean twoByte = false;
        for (int i = 1; i < suffixDeltas.length; i++) {
            if (suffixDeltas[i] > 254) {
                twoByte = true;
            }
        }
        if (twoByte) {
            headerBuffer.writeByte((byte) 255);
            for (int i = 1; i < suffixDeltas.length; i++) {
                headerBuffer.writeShort((short) suffixDeltas[i]);
            }
        } else {
            for (int i = 1; i < suffixDeltas.length; i++) {
                headerBuffer.writeByte((byte) suffixDeltas[i]);
            }
        }
        headerBuffer.copyTo(data);
        headerBuffer.reset();
        bytesBuffer.copyTo(data);
        bytesBuffer.reset();
    }

    // writes reverse term index: used for binary searching a term into a range of 64 blocks
    // for every 64 blocks (1024 terms) we store a term, trimming any suffix unnecessary for comparison
    // terms are written as a contiguous byte[], but never spanning 2^15 byte boundaries.
    private void addReverseTermIndex(FieldInfo field, final Iterable<BytesRef> values, int maxLength) throws IOException {
        long count = 0;
        BytesRefBuilder priorTerm = new BytesRefBuilder();
        priorTerm.grow(maxLength);
        BytesRef indexTerm = new BytesRef();
        long startFP = data.getFilePointer();
        PagedBytes pagedBytes = new PagedBytes(15);
        MonotonicBlockPackedWriter addresses = new MonotonicBlockPackedWriter(data, MONOTONIC_BLOCK_SIZE);

        for (BytesRef b : values) {
            int termPosition = (int) (count & REVERSE_INTERVAL_MASK);
            if (termPosition == 0) {
                int len = LegacyStringHelper.sortKeyLength(priorTerm.get(), b);
                indexTerm.bytes = b.bytes;
                indexTerm.offset = b.offset;
                indexTerm.length = len;
                addresses.add(pagedBytes.copyUsingLengthPrefix(indexTerm));
            } else if (termPosition == REVERSE_INTERVAL_MASK) {
                priorTerm.copyBytes(b);
            }
            count++;
        }
        addresses.finish();
        long numBytes = pagedBytes.getPointer();
        pagedBytes.freeze(true);
        PagedBytes.PagedBytesDataInput in = pagedBytes.getDataInput();
        meta.writeLong(startFP);
        data.writeVLong(numBytes);
        data.copyBytes(in, numBytes);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeVInt(field.number);
        meta.writeByte(Lucene54DocValuesFormat.SORTED);
        addTermsDict(field, LegacyDocValuesIterables.valuesIterable(valuesProducer.getSorted(field)));
        addNumericField(field, LegacyDocValuesIterables.sortedOrdIterable(valuesProducer, field, maxDoc), NumberType.ORDINAL);
    }

    private void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> ords) throws IOException {
        meta.writeVInt(field.number);
        meta.writeByte(Lucene54DocValuesFormat.SORTED);
        addTermsDict(field, values);
        addNumericField(field, ords, NumberType.ORDINAL);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, final DocValuesProducer valuesProducer) throws IOException {

        final Iterable<Number> docToValueCount = LegacyDocValuesIterables.sortedNumericToDocCount(valuesProducer, field, maxDoc);
        final Iterable<Number> values = LegacyDocValuesIterables.sortedNumericToValues(valuesProducer, field);

        meta.writeVInt(field.number);
        meta.writeByte(Lucene54DocValuesFormat.SORTED_NUMERIC);
        if (isSingleValued(docToValueCount)) {
            meta.writeVInt(SORTED_SINGLE_VALUED);
            // The field is single-valued, we can encode it as NUMERIC
            addNumericField(field, singletonView(docToValueCount, values, null), NumberType.VALUE);
        } else {
            final SortedSet<LongsRef> uniqueValueSets = uniqueValueSets(docToValueCount, values);
            if (uniqueValueSets != null) {
                meta.writeVInt(SORTED_SET_TABLE);

                // write the set_id -> values mapping
                writeDictionary(uniqueValueSets);

                // write the doc -> set_id as a numeric field
                addNumericField(field, docToSetId(uniqueValueSets, docToValueCount, values), NumberType.ORDINAL);
            } else {
                meta.writeVInt(SORTED_WITH_ADDRESSES);
                // write the stream of values as a numeric field
                addNumericField(field, values, NumberType.VALUE);
                // write the doc -> ord count as a absolute index to the stream
                addOrdIndex(field, docToValueCount);
            }
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {

        Iterable<BytesRef> values = LegacyDocValuesIterables.valuesIterable(valuesProducer.getSortedSet(field));
        Iterable<Number> docToOrdCount = LegacyDocValuesIterables.sortedSetOrdCountIterable(valuesProducer, field, maxDoc);
        Iterable<Number> ords = LegacyDocValuesIterables.sortedSetOrdsIterable(valuesProducer, field);

        meta.writeVInt(field.number);
        meta.writeByte(Lucene54DocValuesFormat.SORTED_SET);

        if (isSingleValued(docToOrdCount)) {
            meta.writeVInt(SORTED_SINGLE_VALUED);
            // The field is single-valued, we can encode it as SORTED
            addSortedField(field, values, singletonView(docToOrdCount, ords, -1L));
        } else {
            final SortedSet<LongsRef> uniqueValueSets = uniqueValueSets(docToOrdCount, ords);
            if (uniqueValueSets != null) {
                meta.writeVInt(SORTED_SET_TABLE);

                // write the set_id -> ords mapping
                writeDictionary(uniqueValueSets);

                // write the ord -> byte[] as a binary field
                addTermsDict(field, values);

                // write the doc -> set_id as a numeric field
                addNumericField(field, docToSetId(uniqueValueSets, docToOrdCount, ords), NumberType.ORDINAL);
            } else {
                meta.writeVInt(SORTED_WITH_ADDRESSES);

                // write the ord -> byte[] as a binary field
                addTermsDict(field, values);

                // write the stream of ords as a numeric field
                // NOTE: we could return an iterator that delta-encodes these within a doc
                addNumericField(field, ords, NumberType.ORDINAL);

                // write the doc -> ord count as a absolute index to the stream
                addOrdIndex(field, docToOrdCount);
            }
        }
    }

    private SortedSet<LongsRef> uniqueValueSets(Iterable<Number> docToValueCount, Iterable<Number> values) {
        Set<LongsRef> uniqueValueSet = new HashSet<>();
        LongsRef docValues = new LongsRef(256);

        Iterator<Number> valueCountIterator = docToValueCount.iterator();
        Iterator<Number> valueIterator = values.iterator();
        int totalDictSize = 0;
        while (valueCountIterator.hasNext()) {
            docValues.length = valueCountIterator.next().intValue();
            if (docValues.length > 256) {
                return null;
            }
            for (int i = 0; i < docValues.length; ++i) {
                docValues.longs[i] = valueIterator.next().longValue();
            }
            if (uniqueValueSet.contains(docValues)) {
                continue;
            }
            totalDictSize += docValues.length;
            if (totalDictSize > 256) {
                return null;
            }
            uniqueValueSet.add(new LongsRef(ArrayUtil.copyOfSubArray(docValues.longs, 0, docValues.length), 0, docValues.length));
        }
        assert valueIterator.hasNext() == false;
        return new TreeSet<>(uniqueValueSet);
    }

    private void writeDictionary(SortedSet<LongsRef> uniqueValueSets) throws IOException {
        int lengthSum = 0;
        for (LongsRef longs : uniqueValueSets) {
            lengthSum += longs.length;
        }

        meta.writeInt(lengthSum);
        for (LongsRef valueSet : uniqueValueSets) {
            for (int i = 0; i < valueSet.length; ++i) {
                meta.writeLong(valueSet.longs[valueSet.offset + i]);
            }
        }

        meta.writeInt(uniqueValueSets.size());
        for (LongsRef valueSet : uniqueValueSets) {
            meta.writeInt(valueSet.length);
        }
    }

    private Iterable<Number> docToSetId(SortedSet<LongsRef> uniqueValueSets, Iterable<Number> docToValueCount, Iterable<Number> values) {
        final Map<LongsRef, Integer> setIds = new HashMap<>();
        int i = 0;
        for (LongsRef set : uniqueValueSets) {
            setIds.put(set, i++);
        }
        assert i == uniqueValueSets.size();

        return new Iterable<Number>() {

            @Override
            public Iterator<Number> iterator() {
                final Iterator<Number> valueCountIterator = docToValueCount.iterator();
                final Iterator<Number> valueIterator = values.iterator();
                final LongsRef docValues = new LongsRef(256);
                return new Iterator<Number>() {

                    @Override
                    public boolean hasNext() {
                        return valueCountIterator.hasNext();
                    }

                    @Override
                    public Number next() {
                        docValues.length = valueCountIterator.next().intValue();
                        for (int i = 0; i < docValues.length; ++i) {
                            docValues.longs[i] = valueIterator.next().longValue();
                        }
                        final Integer id = setIds.get(docValues);
                        assert id != null;
                        return id;
                    }

                };

            }
        };
    }

    // writes addressing information as MONOTONIC_COMPRESSED integer
    private void addOrdIndex(FieldInfo field, Iterable<Number> values) throws IOException {
        meta.writeVInt(field.number);
        meta.writeByte(Lucene54DocValuesFormat.NUMERIC);
        meta.writeVInt(MONOTONIC_COMPRESSED);
        meta.writeLong(-1L);
        meta.writeLong(data.getFilePointer());
        meta.writeVLong(maxDoc);
        meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

        final LegacyDirectMonotonicWriter writer = LegacyDirectMonotonicWriter.getInstance(
            meta,
            data,
            maxDoc + 1,
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );
        long addr = 0;
        writer.add(addr);
        for (Number v : values) {
            addr += v.longValue();
            writer.add(addr);
        }
        writer.finish();
        meta.writeLong(data.getFilePointer());
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
}
