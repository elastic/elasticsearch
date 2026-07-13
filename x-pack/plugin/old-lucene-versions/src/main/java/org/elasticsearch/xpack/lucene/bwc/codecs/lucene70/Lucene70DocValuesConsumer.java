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
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70;

import org.apache.lucene.backward_codecs.packed.LegacyDirectMonotonicWriter;
import org.apache.lucene.backward_codecs.packed.LegacyDirectWriter;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.Lucene70DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.Lucene70DocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.Lucene70DocValuesFormat.NUMERIC_BLOCK_SIZE;

/** writer for {@link Lucene70DocValuesFormat} */
final class Lucene70DocValuesConsumer extends DocValuesConsumer {

    IndexOutput data, meta;
    final int maxDoc;

    /** expert: Creates a new writer */
    Lucene70DocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = EndiannessReverserUtil.createOutput(state.directory, dataName, state.context);
            CodecUtil.writeIndexHeader(
                data,
                dataCodec,
                Lucene70DocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = EndiannessReverserUtil.createOutput(state.directory, metaName, state.context);
            CodecUtil.writeIndexHeader(
                meta,
                metaCodec,
                Lucene70DocValuesFormat.VERSION_CURRENT,
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
    public void close() throws IOException {
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeInt(-1); // write EOF marker
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

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(Lucene70DocValuesFormat.NUMERIC);

        writeValues(field, new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                return DocValues.singleton(valuesProducer.getNumeric(field));
            }
        });
    }

    private static class MinMaxTracker {
        long min, max, numValues, spaceInBits;

        MinMaxTracker() {
            reset();
            spaceInBits = 0;
        }

        private void reset() {
            min = Long.MAX_VALUE;
            max = Long.MIN_VALUE;
            numValues = 0;
        }

        /** Accumulate a new value. */
        void update(long v) {
            min = Math.min(min, v);
            max = Math.max(max, v);
            ++numValues;
        }

        /** Update the required space. */
        void finish() {
            if (max > min) {
                spaceInBits += LegacyDirectWriter.unsignedBitsRequired(max - min) * numValues;
            }
        }

        /** Update space usage and get ready for accumulating values for the next block. */
        void nextBlock() {
            finish();
            reset();
        }
    }

    private long[] writeValues(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
        int numDocsWithValue = 0;
        MinMaxTracker minMax = new MinMaxTracker();
        MinMaxTracker blockMinMax = new MinMaxTracker();
        long gcd = 0;
        Set<Long> uniqueValues = new HashSet<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                long v = values.nextValue();

                if (gcd != 1) {
                    if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
                        // in that case v - minValue might overflow and make the GCD computation return
                        // wrong results. Since these extreme values are unlikely, we just discard
                        // GCD computation for them
                        gcd = 1;
                    } else if (minMax.numValues != 0) { // minValue needs to be set first
                        gcd = MathUtil.gcd(gcd, v - minMax.min);
                    }
                }

                minMax.update(v);
                blockMinMax.update(v);
                if (blockMinMax.numValues == NUMERIC_BLOCK_SIZE) {
                    blockMinMax.nextBlock();
                }

                if (uniqueValues != null && uniqueValues.add(v) && uniqueValues.size() > 256) {
                    uniqueValues = null;
                }
            }

            numDocsWithValue++;
        }

        minMax.finish();
        blockMinMax.finish();

        final long numValues = minMax.numValues;
        long min = minMax.min;
        final long max = minMax.max;
        assert blockMinMax.spaceInBits <= minMax.spaceInBits;

        if (numDocsWithValue == 0) {
            meta.writeLong(-2);
            meta.writeLong(0L);
        } else if (numDocsWithValue == maxDoc) {
            meta.writeLong(-1);
            meta.writeLong(0L);
        } else {
            long offset = data.getFilePointer();
            meta.writeLong(offset);
            values = valuesProducer.getSortedNumeric(field);
            IndexedDISI.writeBitSet(values, data);
            meta.writeLong(data.getFilePointer() - offset);
        }

        meta.writeLong(numValues);
        final int numBitsPerValue;
        boolean doBlocks = false;
        Map<Long, Integer> encode = null;
        if (min >= max) {
            numBitsPerValue = 0;
            meta.writeInt(-1);
        } else {
            if (uniqueValues != null
                && uniqueValues.size() > 1
                && LegacyDirectWriter.unsignedBitsRequired(uniqueValues.size() - 1) < LegacyDirectWriter.unsignedBitsRequired(
                    (max - min) / gcd
                )) {
                numBitsPerValue = LegacyDirectWriter.unsignedBitsRequired(uniqueValues.size() - 1);
                final Long[] sortedUniqueValues = uniqueValues.toArray(new Long[0]);
                Arrays.sort(sortedUniqueValues);
                meta.writeInt(sortedUniqueValues.length);
                for (Long v : sortedUniqueValues) {
                    meta.writeLong(v);
                }
                encode = new HashMap<>();
                for (int i = 0; i < sortedUniqueValues.length; ++i) {
                    encode.put(sortedUniqueValues[i], i);
                }
                min = 0;
                gcd = 1;
            } else {
                uniqueValues = null;
                // we do blocks if that appears to save 10+% storage
                doBlocks = minMax.spaceInBits > 0 && (double) blockMinMax.spaceInBits / minMax.spaceInBits <= 0.9;
                if (doBlocks) {
                    numBitsPerValue = 0xFF;
                    meta.writeInt(-2 - NUMERIC_BLOCK_SHIFT);
                } else {
                    numBitsPerValue = LegacyDirectWriter.unsignedBitsRequired((max - min) / gcd);
                    if (gcd == 1
                        && min > 0
                        && LegacyDirectWriter.unsignedBitsRequired(max) == LegacyDirectWriter.unsignedBitsRequired(max - min)) {
                        min = 0;
                    }
                    meta.writeInt(-1);
                }
            }
        }

        meta.writeByte((byte) numBitsPerValue);
        meta.writeLong(min);
        meta.writeLong(gcd);
        long startOffset = data.getFilePointer();
        meta.writeLong(startOffset);
        if (doBlocks) {
            writeValuesMultipleBlocks(valuesProducer.getSortedNumeric(field), gcd);
        } else if (numBitsPerValue != 0) {
            writeValuesSingleBlock(valuesProducer.getSortedNumeric(field), numValues, numBitsPerValue, min, gcd, encode);
        }
        meta.writeLong(data.getFilePointer() - startOffset);

        return new long[] { numDocsWithValue, numValues };
    }

    private void writeValuesSingleBlock(
        SortedNumericDocValues values,
        long numValues,
        int numBitsPerValue,
        long min,
        long gcd,
        Map<Long, Integer> encode
    ) throws IOException {
        LegacyDirectWriter writer = LegacyDirectWriter.getInstance(data, numValues, numBitsPerValue);
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                long v = values.nextValue();
                if (encode == null) {
                    writer.add((v - min) / gcd);
                } else {
                    writer.add(encode.get(v));
                }
            }
        }
        writer.finish();
    }

    private void writeValuesMultipleBlocks(SortedNumericDocValues values, long gcd) throws IOException {
        final long[] buffer = new long[NUMERIC_BLOCK_SIZE];
        final ByteBuffersDataOutput encodeBuffer = ByteBuffersDataOutput.newResettableInstance();
        int upTo = 0;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                buffer[upTo++] = values.nextValue();
                if (upTo == NUMERIC_BLOCK_SIZE) {
                    writeBlock(buffer, NUMERIC_BLOCK_SIZE, gcd, encodeBuffer);
                    upTo = 0;
                }
            }
        }
        if (upTo > 0) {
            writeBlock(buffer, upTo, gcd, encodeBuffer);
        }
    }

    private void writeBlock(long[] values, int length, long gcd, ByteBuffersDataOutput buffer) throws IOException {
        assert length > 0;
        long min = values[0];
        long max = values[0];
        for (int i = 1; i < length; ++i) {
            final long v = values[i];
            assert Math.floorMod(values[i] - min, gcd) == 0;
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        if (min == max) {
            data.writeByte((byte) 0);
            data.writeLong(min);
        } else {
            final int bitsPerValue = LegacyDirectWriter.unsignedBitsRequired(max - min);
            buffer.reset();
            assert buffer.size() == 0;
            final LegacyDirectWriter w = LegacyDirectWriter.getInstance(buffer, length, bitsPerValue);
            for (int i = 0; i < length; ++i) {
                w.add((values[i] - min) / gcd);
            }
            w.finish();
            data.writeByte((byte) bitsPerValue);
            data.writeLong(min);
            data.writeInt(Math.toIntExact(buffer.size()));
            buffer.copyTo(data);
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(Lucene70DocValuesFormat.BINARY);

        BinaryDocValues values = valuesProducer.getBinary(field);
        long start = data.getFilePointer();
        meta.writeLong(start);
        int numDocsWithField = 0;
        int minLength = Integer.MAX_VALUE;
        int maxLength = 0;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            numDocsWithField++;
            BytesRef v = values.binaryValue();
            int length = v.length;
            data.writeBytes(v.bytes, v.offset, v.length);
            minLength = Math.min(length, minLength);
            maxLength = Math.max(length, maxLength);
        }
        assert numDocsWithField <= maxDoc;
        meta.writeLong(data.getFilePointer() - start);

        if (numDocsWithField == 0) {
            meta.writeLong(-2);
            meta.writeLong(0L);
        } else if (numDocsWithField == maxDoc) {
            meta.writeLong(-1);
            meta.writeLong(0L);
        } else {
            long offset = data.getFilePointer();
            meta.writeLong(offset);
            values = valuesProducer.getBinary(field);
            IndexedDISI.writeBitSet(values, data);
            meta.writeLong(data.getFilePointer() - offset);
        }

        meta.writeInt(numDocsWithField);
        meta.writeInt(minLength);
        meta.writeInt(maxLength);
        if (maxLength > minLength) {
            start = data.getFilePointer();
            meta.writeLong(start);
            meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

            final LegacyDirectMonotonicWriter writer = LegacyDirectMonotonicWriter.getInstance(
                meta,
                data,
                numDocsWithField + 1,
                DIRECT_MONOTONIC_BLOCK_SHIFT
            );
            long addr = 0;
            writer.add(addr);
            values = valuesProducer.getBinary(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                addr += values.binaryValue().length;
                writer.add(addr);
            }
            writer.finish();
            meta.writeLong(data.getFilePointer() - start);
        }
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(Lucene70DocValuesFormat.SORTED);
        doAddSortedField(field, valuesProducer);
    }

    private void doAddSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        SortedDocValues values = valuesProducer.getSorted(field);
        int numDocsWithField = 0;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            numDocsWithField++;
        }

        if (numDocsWithField == 0) {
            meta.writeLong(-2);
            meta.writeLong(0L);
        } else if (numDocsWithField == maxDoc) {
            meta.writeLong(-1);
            meta.writeLong(0L);
        } else {
            long offset = data.getFilePointer();
            meta.writeLong(offset);
            values = valuesProducer.getSorted(field);
            IndexedDISI.writeBitSet(values, data);
            meta.writeLong(data.getFilePointer() - offset);
        }

        meta.writeInt(numDocsWithField);
        if (values.getValueCount() <= 1) {
            meta.writeByte((byte) 0);
            meta.writeLong(0L);
            meta.writeLong(0L);
        } else {
            int numberOfBitsPerOrd = LegacyDirectWriter.unsignedBitsRequired(values.getValueCount() - 1);
            meta.writeByte((byte) numberOfBitsPerOrd);
            long start = data.getFilePointer();
            meta.writeLong(start);
            LegacyDirectWriter writer = LegacyDirectWriter.getInstance(data, numDocsWithField, numberOfBitsPerOrd);
            values = valuesProducer.getSorted(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                writer.add(values.ordValue());
            }
            writer.finish();
            meta.writeLong(data.getFilePointer() - start);
        }

        addTermsDict(DocValues.singleton(valuesProducer.getSorted(field)));
    }

    private void addTermsDict(SortedSetDocValues values) throws IOException {
        final long size = values.getValueCount();
        meta.writeVLong(size);
        meta.writeInt(Lucene70DocValuesFormat.TERMS_DICT_BLOCK_SHIFT);

        ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput addressIndexOut = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
        meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
        long numBlocks = (size + Lucene70DocValuesFormat.TERMS_DICT_BLOCK_MASK) >>> Lucene70DocValuesFormat.TERMS_DICT_BLOCK_SHIFT;
        LegacyDirectMonotonicWriter writer = LegacyDirectMonotonicWriter.getInstance(
            meta,
            addressIndexOut,
            numBlocks,
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );

        BytesRefBuilder previous = new BytesRefBuilder();
        long ord = 0;
        long start = data.getFilePointer();
        int maxLength = 0;
        TermsEnum iterator = values.termsEnum();
        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
            if ((ord & Lucene70DocValuesFormat.TERMS_DICT_BLOCK_MASK) == 0) {
                writer.add(data.getFilePointer() - start);
                data.writeVInt(term.length);
                data.writeBytes(term.bytes, term.offset, term.length);
            } else {
                final int prefixLength = StringHelper.bytesDifference(previous.get(), term);
                final int suffixLength = term.length - prefixLength;
                assert suffixLength > 0; // terms are unique

                data.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));
                if (prefixLength >= 15) {
                    data.writeVInt(prefixLength - 15);
                }
                if (suffixLength >= 16) {
                    data.writeVInt(suffixLength - 16);
                }
                data.writeBytes(term.bytes, term.offset + prefixLength, term.length - prefixLength);
            }
            maxLength = Math.max(maxLength, term.length);
            previous.copyBytes(term);
            ++ord;
        }
        writer.finish();
        meta.writeInt(maxLength);
        meta.writeLong(start);
        meta.writeLong(data.getFilePointer() - start);
        start = data.getFilePointer();
        addressBuffer.copyTo(data);
        meta.writeLong(start);
        meta.writeLong(data.getFilePointer() - start);

        // Now write the reverse terms index
        writeTermsIndex(values);
    }

    private void writeTermsIndex(SortedSetDocValues values) throws IOException {
        final long size = values.getValueCount();
        meta.writeInt(Lucene70DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
        long start = data.getFilePointer();

        long numBlocks = 1L + ((size + Lucene70DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK)
            >>> Lucene70DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
        ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput addressIndexOut = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
        LegacyDirectMonotonicWriter writer = LegacyDirectMonotonicWriter.getInstance(
            meta,
            addressIndexOut,
            numBlocks,
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );

        TermsEnum iterator = values.termsEnum();
        BytesRefBuilder previous = new BytesRefBuilder();
        long offset = 0;
        long ord = 0;
        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
            if ((ord & Lucene70DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == 0) {
                writer.add(offset);
                final int sortKeyLength;
                if (ord == 0) {
                    // no previous term: no bytes to write
                    sortKeyLength = 0;
                } else {
                    sortKeyLength = StringHelper.sortKeyLength(previous.get(), term);
                }
                offset += sortKeyLength;
                data.writeBytes(term.bytes, term.offset, sortKeyLength);
            } else if ((ord
                & Lucene70DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == Lucene70DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) {
                    previous.copyBytes(term);
                }
            ++ord;
        }
        writer.add(offset);
        writer.finish();
        meta.writeLong(start);
        meta.writeLong(data.getFilePointer() - start);
        start = data.getFilePointer();
        addressBuffer.copyTo(data);
        meta.writeLong(start);
        meta.writeLong(data.getFilePointer() - start);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(Lucene70DocValuesFormat.SORTED_NUMERIC);

        long[] stats = writeValues(field, valuesProducer);
        int numDocsWithField = Math.toIntExact(stats[0]);
        long numValues = stats[1];
        assert numValues >= numDocsWithField;

        meta.writeInt(numDocsWithField);
        if (numValues > numDocsWithField) {
            long start = data.getFilePointer();
            meta.writeLong(start);
            meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

            final LegacyDirectMonotonicWriter addressesWriter = LegacyDirectMonotonicWriter.getInstance(
                meta,
                data,
                numDocsWithField + 1L,
                DIRECT_MONOTONIC_BLOCK_SHIFT
            );
            long addr = 0;
            addressesWriter.add(addr);
            SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                addr += values.docValueCount();
                addressesWriter.add(addr);
            }
            addressesWriter.finish();
            meta.writeLong(data.getFilePointer() - start);
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(Lucene70DocValuesFormat.SORTED_SET);

        SortedSetDocValues values = valuesProducer.getSortedSet(field);
        int numDocsWithField = 0;
        long numOrds = 0;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            numDocsWithField++;
            numOrds += values.docValueCount();
        }

        if (numDocsWithField == numOrds) {
            meta.writeByte((byte) 0);
            doAddSortedField(field, new EmptyDocValuesProducer() {
                @Override
                public SortedDocValues getSorted(FieldInfo field) throws IOException {
                    return SortedSetSelector.wrap(valuesProducer.getSortedSet(field), SortedSetSelector.Type.MIN);
                }
            });
            return;
        }
        meta.writeByte((byte) 1);

        assert numDocsWithField != 0;
        if (numDocsWithField == maxDoc) {
            meta.writeLong(-1);
            meta.writeLong(0L);
        } else {
            long offset = data.getFilePointer();
            meta.writeLong(offset);
            values = valuesProducer.getSortedSet(field);
            IndexedDISI.writeBitSet(values, data);
            meta.writeLong(data.getFilePointer() - offset);
        }

        int numberOfBitsPerOrd = LegacyDirectWriter.unsignedBitsRequired(values.getValueCount() - 1);
        meta.writeByte((byte) numberOfBitsPerOrd);
        long start = data.getFilePointer();
        meta.writeLong(start);
        LegacyDirectWriter writer = LegacyDirectWriter.getInstance(data, numOrds, numberOfBitsPerOrd);
        values = valuesProducer.getSortedSet(field);
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            for (int i = 0; i < values.docValueCount(); i++) {
                writer.add(values.nextOrd());
            }
        }
        writer.finish();
        meta.writeLong(data.getFilePointer() - start);

        meta.writeInt(numDocsWithField);
        start = data.getFilePointer();
        meta.writeLong(start);
        meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

        final LegacyDirectMonotonicWriter addressesWriter = LegacyDirectMonotonicWriter.getInstance(
            meta,
            data,
            numDocsWithField + 1,
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );
        long addr = 0;
        addressesWriter.add(addr);
        values = valuesProducer.getSortedSet(field);
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            values.nextOrd();
            addr += values.docValueCount();
            addressesWriter.add(addr);
        }
        addressesWriter.finish();
        meta.writeLong(data.getFilePointer() - start);

        addTermsDict(values);
    }
}
