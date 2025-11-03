/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldDataInput;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.index.codec.bloomfilter.BloomFilterHashFunctions.MurmurHash3.hash64;

/**
 * A stored fields format that builds a Bloom filter for a specific field to enable fast
 * existence checks, while delegating storage of all other fields to another StoredFieldsFormat.
 *
 * <p>The field used to build the Bloom filter is not stored, as only its presence
 * needs to be tracked for filtering purposes. This reduces storage overhead while maintaining
 * the ability to quickly determine if a segment might contain the field.
 *
 * <p><b>File formats</b>
 *
 * <p>Bloom filter stored fields are represented by two files:
 *
 * <ol>
 *   <li>
 *       <p>Bloom filter data file (extension .sfbf). This file stores the bloom
 *       filter bitset.
 *   <li>
 *       <p>A bloom filter meta file (extension .sfbfm). This file stores metadata about the
 *       bloom filters stored in the bloom filter data file. The in-memory representation can
 *       be found in {@link BloomFilterMetadata}.
 * </ol>
 */
public class ES93BloomFilterStoredFieldsFormat extends StoredFieldsFormat {
    public static final String STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME = "ES93BloomFilterStoredFieldsFormat";
    public static final String STORED_FIELDS_BLOOM_FILTER_EXTENSION = "sfbf";
    public static final String STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION = "sfbfm";
    private static final int VERSION_START = 0;
    private static final int VERSION_CURRENT = VERSION_START;

    // We use prime numbers with the Kirsch-Mitzenmacher technique to obtain multiple hashes from two hash functions
    private static final int[] PRIMES = new int[] { 2, 5, 11, 17, 23, 29, 41, 47, 53, 59, 71 };
    private static final int DEFAULT_NUM_HASH_FUNCTIONS = 7;
    private static final byte BLOOM_FILTER_STORED = 1;
    private static final byte BLOOM_FILTER_NOT_STORED = 0;
    private static final ByteSizeValue MAX_BLOOM_FILTER_SIZE = ByteSizeValue.ofMb(8);

    private final BigArrays bigArrays;
    private final String segmentSuffix;
    private final StoredFieldsFormat delegate;
    private final String bloomFilterFieldName;
    private final int numHashFunctions;
    private final int bloomFilterSizeInBits;

    public ES93BloomFilterStoredFieldsFormat(
        BigArrays bigArrays,
        String segmentSuffix,
        StoredFieldsFormat delegate,
        ByteSizeValue bloomFilterSize,
        String bloomFilterFieldName
    ) {
        this.bigArrays = bigArrays;
        this.segmentSuffix = segmentSuffix;
        this.delegate = delegate;
        this.bloomFilterFieldName = bloomFilterFieldName;
        this.numHashFunctions = DEFAULT_NUM_HASH_FUNCTIONS;

        if (bloomFilterSize.getBytes() <= 0) {
            throw new IllegalArgumentException("bloom filter size must be greater than 0");
        }

        var closestPowerOfTwoBloomFilterSizeInBytes = Long.highestOneBit(bloomFilterSize.getBytes());
        if (closestPowerOfTwoBloomFilterSizeInBytes > MAX_BLOOM_FILTER_SIZE.getBytes()) {
            throw new IllegalArgumentException(
                "bloom filter size ["
                    + bloomFilterSize
                    + "] is too large; "
                    + "must be "
                    + MAX_BLOOM_FILTER_SIZE
                    + " or less (rounded to nearest power of two)"
            );
        }
        this.bloomFilterSizeInBits = Math.toIntExact(Math.multiplyExact(closestPowerOfTwoBloomFilterSizeInBytes, Byte.SIZE));
    }

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        return new Reader(directory, si, fn, context, segmentSuffix, delegate.fieldsReader(directory, si, fn, context));
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        // TODO: compute the bloom filter size based on heuristics and oversize factor
        return new Writer(
            directory,
            si,
            context,
            segmentSuffix,
            bigArrays,
            numHashFunctions,
            bloomFilterSizeInBits,
            bloomFilterFieldName,
            delegate.fieldsWriter(directory, si, context)
        );
    }

    static class Writer extends StoredFieldsWriter {
        private final IndexOutput bloomFilterDataOut;
        private final IndexOutput metadataOut;
        private final ByteArray buffer;
        private final List<Closeable> toClose = new ArrayList<>();
        private final int[] hashes;
        private final int numHashFunctions;
        private final int bloomFilterSizeInBits;
        private final int bloomFilterSizeInBytes;
        private final StoredFieldsWriter delegateWriter;
        private final String bloomFilterFieldName;
        private FieldInfo bloomFilterFieldInfo;

        Writer(
            Directory directory,
            SegmentInfo segmentInfo,
            IOContext context,
            String segmentSuffix,
            BigArrays bigArrays,
            int numHashFunctions,
            int bloomFilterSizeInBits,
            String bloomFilterFieldName,
            StoredFieldsWriter delegateWriter
        ) throws IOException {
            assert isPowerOfTwo(bloomFilterSizeInBits) : "Bloom filter size is not a power of 2: " + bloomFilterSizeInBits;
            assert numHashFunctions <= PRIMES.length
                : "Number of hash functions must be <= " + PRIMES.length + " but was " + numHashFunctions;

            this.numHashFunctions = numHashFunctions;
            this.hashes = new int[numHashFunctions];
            this.bloomFilterSizeInBits = bloomFilterSizeInBits;
            this.bloomFilterSizeInBytes = bloomFilterSizeInBits / Byte.SIZE;
            this.bloomFilterFieldName = bloomFilterFieldName;

            this.delegateWriter = delegateWriter;
            toClose.add(delegateWriter);

            boolean success = false;
            try {
                bloomFilterDataOut = directory.createOutput(bloomFilterFileName(segmentInfo, segmentSuffix), context);
                toClose.add(bloomFilterDataOut);
                CodecUtil.writeIndexHeader(
                    bloomFilterDataOut,
                    STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME,
                    VERSION_CURRENT,
                    segmentInfo.getId(),
                    segmentSuffix
                );

                metadataOut = directory.createOutput(bloomFilterMetadataFileName(segmentInfo, segmentSuffix), context);
                toClose.add(metadataOut);
                CodecUtil.writeIndexHeader(
                    metadataOut,
                    STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME,
                    VERSION_CURRENT,
                    segmentInfo.getId(),
                    segmentSuffix
                );

                buffer = bigArrays.newByteArray(bloomFilterSizeInBytes, false);
                toClose.add(buffer);

                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }
        }

        @Override
        public void startDocument() throws IOException {
            delegateWriter.startDocument();
        }

        @Override
        public void finishDocument() throws IOException {
            delegateWriter.finishDocument();
        }

        @Override
        public void writeField(FieldInfo info, int value) throws IOException {
            if (isBloomFilterField(info) == false) {
                delegateWriter.writeField(info, value);
            }
        }

        @Override
        public void writeField(FieldInfo info, long value) throws IOException {
            if (isBloomFilterField(info) == false) {
                delegateWriter.writeField(info, value);
            }
        }

        @Override
        public void writeField(FieldInfo info, float value) throws IOException {
            if (isBloomFilterField(info) == false) {
                delegateWriter.writeField(info, value);
            }
        }

        @Override
        public void writeField(FieldInfo info, double value) throws IOException {
            if (isBloomFilterField(info) == false) {
                delegateWriter.writeField(info, value);
            }
        }

        @Override
        public void writeField(FieldInfo info, StoredFieldDataInput value) throws IOException {
            if (isBloomFilterField(info) == false) {
                delegateWriter.writeField(info, value);
            }
        }

        @Override
        public void writeField(FieldInfo info, String value) throws IOException {
            if (isBloomFilterField(info) == false) {
                delegateWriter.writeField(info, value);
            }
        }

        @Override
        public void writeField(FieldInfo info, BytesRef value) throws IOException {
            if (isBloomFilterField(info)) {
                addToBloomFilter(info, value);
            } else {
                delegateWriter.writeField(info, value);
            }
        }

        private boolean isBloomFilterField(FieldInfo info) {
            return (bloomFilterFieldInfo != null && bloomFilterFieldInfo.getFieldNumber() == info.getFieldNumber())
                || info.getName().equals(bloomFilterFieldName);
        }

        private void addToBloomFilter(FieldInfo info, BytesRef value) {
            assert info.getName().equals(bloomFilterFieldName) : "Expected " + bloomFilterFieldName + " but got " + info;
            bloomFilterFieldInfo = info;
            var termHashes = hashTerm(value, hashes);
            for (int hash : termHashes) {
                final int posInBitArray = hash & (bloomFilterSizeInBits - 1);
                final int pos = posInBitArray >> 3; // div 8
                final int mask = 1 << (posInBitArray & 7); // mod 8
                final byte val = (byte) (buffer.get(pos) | mask);
                buffer.set(pos, val);
            }
        }

        @Override
        public void finish(int numDocs) throws IOException {
            finishBloomFilterStoredFormat();
            delegateWriter.finish(numDocs);
        }

        private void finishBloomFilterStoredFormat() throws IOException {
            BloomFilterMetadata bloomFilterMetadata = null;
            if (bloomFilterFieldInfo != null) {
                bloomFilterMetadata = new BloomFilterMetadata(
                    bloomFilterFieldInfo,
                    bloomFilterDataOut.getFilePointer(),
                    bloomFilterSizeInBits,
                    numHashFunctions
                );

                if (buffer.hasArray()) {
                    bloomFilterDataOut.writeBytes(buffer.array(), 0, bloomFilterSizeInBytes);
                } else {
                    BytesReference.fromByteArray(buffer, bloomFilterSizeInBytes).writeTo(new IndexOutputOutputStream(bloomFilterDataOut));
                }
            }
            CodecUtil.writeFooter(bloomFilterDataOut);

            if (bloomFilterMetadata != null) {
                metadataOut.writeByte(BLOOM_FILTER_STORED);
                bloomFilterMetadata.writeTo(metadataOut);
            } else {
                metadataOut.writeByte(BLOOM_FILTER_NOT_STORED);
            }
            CodecUtil.writeFooter(metadataOut);
        }

        @Override
        public int merge(MergeState mergeState) throws IOException {
            // Skip merging the bloom filter for now
            finishBloomFilterStoredFormat();
            return delegateWriter.merge(mergeState);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(toClose);
        }

        @Override
        public long ramBytesUsed() {
            return buffer.ramBytesUsed() + delegateWriter.ramBytesUsed();
        }
    }

    private static class Reader extends StoredFieldsReader implements BloomFilterProvider {
        @Nullable
        private final BloomFilterFieldReader bloomFilterFieldReader;
        private final StoredFieldsReader delegateReader;

        Reader(
            Directory directory,
            SegmentInfo si,
            FieldInfos fn,
            IOContext context,
            String segmentSuffix,
            StoredFieldsReader delegateReader
        ) throws IOException {
            this.delegateReader = delegateReader;
            var success = false;
            try {
                bloomFilterFieldReader = BloomFilterFieldReader.open(directory, si, fn, context, segmentSuffix);
                success = true;
            } finally {
                if (success == false) {
                    delegateReader.close();
                }
            }
        }

        @Override
        public StoredFieldsReader clone() {
            return this;
        }

        @Override
        public void checkIntegrity() throws IOException {
            if (bloomFilterFieldReader != null) {
                bloomFilterFieldReader.checkIntegrity();
            }
            delegateReader.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(bloomFilterFieldReader, delegateReader);
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            delegateReader.document(docID, visitor);
        }

        @Override
        public BloomFilter getBloomFilter() throws IOException {
            return bloomFilterFieldReader;
        }
    }

    record BloomFilterMetadata(FieldInfo fieldInfo, long fileOffset, int sizeInBits, int numHashFunctions) {
        BloomFilterMetadata {
            assert fieldInfo != null;
            assert isPowerOfTwo(sizeInBits);
        }

        int sizeInBytes() {
            return sizeInBits / Byte.SIZE;
        }

        void writeTo(IndexOutput indexOut) throws IOException {
            indexOut.writeVInt(fieldInfo.number);
            indexOut.writeVLong(fileOffset);
            indexOut.writeVInt(sizeInBits);
            indexOut.writeVInt(numHashFunctions);
        }

        static BloomFilterMetadata readFrom(IndexInput in, FieldInfos fieldInfos) throws IOException {
            final var fieldInfo = fieldInfos.fieldInfo(in.readVInt());
            final long fileOffset = in.readVLong();
            final int bloomFilterSizeInBits = in.readVInt();
            final int numOfHashFunctions = in.readVInt();
            return new BloomFilterMetadata(fieldInfo, fileOffset, bloomFilterSizeInBits, numOfHashFunctions);
        }
    }

    static class BloomFilterFieldReader implements BloomFilter {
        private final FieldInfo fieldInfo;
        private final IndexInput bloomFilterData;
        private final RandomAccessInput bloomFilterIn;
        private final int bloomFilterSizeInBits;
        private final int[] hashes;

        @Nullable
        static BloomFilterFieldReader open(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context, String segmentSuffix)
            throws IOException {
            List<Closeable> toClose = new ArrayList<>();
            boolean success = false;
            try (var metaInput = directory.openChecksumInput(bloomFilterMetadataFileName(si, segmentSuffix))) {
                var metadataVersion = CodecUtil.checkIndexHeader(
                    metaInput,
                    STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    si.getId(),
                    segmentSuffix
                );
                var hasBloomFilter = metaInput.readByte() == BLOOM_FILTER_STORED;
                if (hasBloomFilter == false) {
                    return null;
                }
                BloomFilterMetadata bloomFilterMetadata = BloomFilterMetadata.readFrom(metaInput, fn);
                CodecUtil.checkFooter(metaInput);

                IndexInput bloomFilterData = directory.openInput(bloomFilterFileName(si, segmentSuffix), context);
                toClose.add(bloomFilterData);
                var bloomFilterDataVersion = CodecUtil.checkIndexHeader(
                    bloomFilterData,
                    STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    si.getId(),
                    segmentSuffix
                );

                if (metadataVersion != bloomFilterDataVersion) {
                    throw new CorruptIndexException(
                        "Format versions mismatch: meta=" + metadataVersion + ", data=" + bloomFilterDataVersion,
                        bloomFilterData
                    );
                }
                CodecUtil.checksumEntireFile(bloomFilterData);

                var bloomFilterFieldReader = new BloomFilterFieldReader(
                    bloomFilterMetadata.fieldInfo(),
                    bloomFilterData.randomAccessSlice(bloomFilterMetadata.fileOffset(), bloomFilterMetadata.sizeInBytes()),
                    bloomFilterMetadata.sizeInBits(),
                    bloomFilterMetadata.numHashFunctions(),
                    bloomFilterData
                );
                success = true;
                return bloomFilterFieldReader;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }
        }

        BloomFilterFieldReader(
            FieldInfo fieldInfo,
            RandomAccessInput bloomFilterIn,
            int bloomFilterSizeInBits,
            int numHashFunctions,
            IndexInput bloomFilterData
        ) {
            this.fieldInfo = Objects.requireNonNull(fieldInfo);
            this.bloomFilterIn = bloomFilterIn;
            this.bloomFilterSizeInBits = bloomFilterSizeInBits;
            this.hashes = new int[numHashFunctions];
            this.bloomFilterData = bloomFilterData;
        }

        public boolean mayContainTerm(String field, BytesRef term) throws IOException {
            assert fieldInfo.getName().equals(field);

            var termHashes = hashTerm(term, hashes);

            for (int hash : termHashes) {
                final int posInBitArray = hash & (bloomFilterSizeInBits - 1);
                final int pos = posInBitArray >> 3; // div 8
                final int mask = 1 << (posInBitArray & 7); // mod 8
                final byte bits = bloomFilterIn.readByte(pos);
                if ((bits & mask) == 0) {
                    return false;
                }
            }
            return true;
        }

        void checkIntegrity() throws IOException {
            CodecUtil.checksumEntireFile(bloomFilterData);
        }

        @Override
        public void close() throws IOException {
            bloomFilterData.close();
        }
    }

    private static int[] hashTerm(BytesRef value, int[] outputs) {
        long hash64 = hash64(value.bytes, value.offset, value.length);
        // First use output splitting to get two hash values out of a single hash function
        int upperHalf = (int) (hash64 >> Integer.SIZE);
        int lowerHalf = (int) hash64;
        // Then use the Kirsch-Mitzenmacher technique to obtain multiple hashes efficiently
        for (int i = 0; i < outputs.length; i++) {
            // Use prime numbers as the constant for the KM technique so these don't have a common gcd
            outputs[i] = (lowerHalf + PRIMES[i] * upperHalf) & 0x7FFF_FFFF; // Clears sign bit, gives positive 31-bit values
        }
        return outputs;
    }

    private static boolean isPowerOfTwo(int value) {
        return (value & (value - 1)) == 0;
    }

    private static String bloomFilterMetadataFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION);
    }

    private static String bloomFilterFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_BLOOM_FILTER_EXTENSION);
    }

    public interface BloomFilter extends Closeable {
        /**
         * Tests whether the given term may exist in the specified field.
         *
         * @param field the field name to check
         * @param term the term to test for membership
         * @return true if term may be present, false if definitely absent
         */
        boolean mayContainTerm(String field, BytesRef term) throws IOException;
    }

    public interface BloomFilterProvider extends Closeable {
        @Nullable
        BloomFilter getBloomFilter() throws IOException;
    }
}
