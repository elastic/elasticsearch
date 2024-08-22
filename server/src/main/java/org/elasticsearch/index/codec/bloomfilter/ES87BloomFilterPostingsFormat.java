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
 * Modifications copyright (C) 2023 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * This implementation is forked from Lucene's BloomFilterPosting to support on-disk bloom filters.
 * <p>
 * A {@link PostingsFormat} useful for low doc-frequency fields such as primary keys. Bloom filters
 * offers "fast-fail" for reads in segments known to have no record of the key.
 */
public class ES87BloomFilterPostingsFormat extends PostingsFormat {
    static final String BLOOM_CODEC_NAME = "ES87BloomFilter";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;
    static final String BLOOM_FILTER_META_FILE = "bfm";
    static final String BLOOM_FILTER_INDEX_FILE = "bfi";
    /** Bloom filters target 10 bits per entry, which, along with 7 hash functions, yields about 1% false positives. */
    private static final int BITS_PER_ENTRY = 10;
    /** The optimal number of hash functions for a bloom filter is approximately 0.7 times the number of bits per entry. */
    private static final int NUM_HASH_FUNCTIONS = 7;

    private Function<String, PostingsFormat> postingsFormats;
    private BigArrays bigArrays;

    public ES87BloomFilterPostingsFormat(BigArrays bigArrays, Function<String, PostingsFormat> postingsFormats) {
        this();
        this.bigArrays = Objects.requireNonNull(bigArrays);
        this.postingsFormats = Objects.requireNonNull(postingsFormats);
    }

    public ES87BloomFilterPostingsFormat() {
        super(BLOOM_CODEC_NAME);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (postingsFormats == null || bigArrays == null) {
            assert false : BLOOM_CODEC_NAME + " was initialized with a wrong constructor";
            throw new UnsupportedOperationException(BLOOM_CODEC_NAME + " was initialized with a wrong constructor");
        }
        return new FieldsWriter(state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FieldsReader(state);
    }

    @Override
    public String toString() {
        return BLOOM_CODEC_NAME;
    }

    private static String metaFile(SegmentInfo si, String segmentSuffix) {
        return IndexFileNames.segmentFileName(si.name, segmentSuffix, BLOOM_FILTER_META_FILE);
    }

    private static String indexFile(SegmentInfo si, String segmentSuffix) {
        return IndexFileNames.segmentFileName(si.name, segmentSuffix, BLOOM_FILTER_INDEX_FILE);
    }

    final class FieldsWriter extends FieldsConsumer {
        private final SegmentWriteState state;
        private final IndexOutput indexOut;
        private final List<BloomFilter> bloomFilters = new ArrayList<>();
        private final List<FieldsGroup> fieldsGroups = new ArrayList<>();
        private final List<Closeable> toCloses = new ArrayList<>();
        private boolean closed;

        FieldsWriter(SegmentWriteState state) throws IOException {
            this.state = state;
            boolean success = false;
            try {
                indexOut = state.directory.createOutput(indexFile(state.segmentInfo, state.segmentSuffix), state.context);
                toCloses.add(indexOut);
                CodecUtil.writeIndexHeader(indexOut, BLOOM_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toCloses);
                }
            }
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
            writePostings(fields, norms);
            writeBloomFilters(fields);
        }

        private void writePostings(Fields fields, NormsProducer norms) throws IOException {
            final Map<PostingsFormat, FieldsGroup> currentGroups = new HashMap<>();
            for (String field : fields) {
                final PostingsFormat postingsFormat = postingsFormats.apply(field);
                if (postingsFormat == null) {
                    throw new IllegalStateException("PostingsFormat for field [" + field + "] wasn't specified");
                }
                FieldsGroup group = currentGroups.get(postingsFormat);
                if (group == null) {
                    group = new FieldsGroup(postingsFormat, Integer.toString(fieldsGroups.size()), new ArrayList<>());
                    currentGroups.put(postingsFormat, group);
                    fieldsGroups.add(group);
                }
                group.fields.add(field);
            }
            for (FieldsGroup group : currentGroups.values()) {
                final FieldsConsumer writer = group.postingsFormat.fieldsConsumer(new SegmentWriteState(state, group.suffix));
                toCloses.add(writer);
                final Fields maskedFields = new FilterLeafReader.FilterFields(fields) {
                    @Override
                    public Iterator<String> iterator() {
                        return group.fields.iterator();
                    }
                };
                writer.write(maskedFields, norms);
            }
        }

        private void writeBloomFilters(Fields fields) throws IOException {
            final int bloomFilterSize = bloomFilterSize(state.segmentInfo.maxDoc());
            final int numBytes = numBytesForBloomFilter(bloomFilterSize);
            final int[] hashes = new int[NUM_HASH_FUNCTIONS];
            try (ByteArray buffer = bigArrays.newByteArray(numBytes, false)) {
                long written = indexOut.getFilePointer();
                for (String field : fields) {
                    final Terms terms = fields.terms(field);
                    if (terms == null) {
                        continue;
                    }
                    buffer.fill(0, numBytes, (byte) 0);
                    final TermsEnum termsEnum = terms.iterator();
                    while (true) {
                        final BytesRef term = termsEnum.next();
                        if (term == null) {
                            break;
                        }
                        for (int hash : hashTerm(term, hashes)) {
                            hash = hash % bloomFilterSize;
                            final int pos = hash >> 3;
                            final int mask = 1 << (hash & 7);
                            final byte val = (byte) (buffer.get(pos) | mask);
                            buffer.set(pos, val);
                        }
                    }
                    bloomFilters.add(new BloomFilter(field, written, bloomFilterSize));
                    if (buffer.hasArray()) {
                        indexOut.writeBytes(buffer.array(), 0, numBytes);
                    } else {
                        BytesReference.fromByteArray(buffer, numBytes).writeTo(new IndexOutputOutputStream(indexOut));
                    }
                    written += numBytes;
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            final long indexFileLength;
            closed = true;
            try {
                CodecUtil.writeFooter(indexOut);
                indexFileLength = indexOut.getFilePointer();
            } finally {
                IOUtils.close(toCloses);
            }
            try (IndexOutput metaOut = state.directory.createOutput(metaFile(state.segmentInfo, state.segmentSuffix), state.context)) {
                CodecUtil.writeIndexHeader(metaOut, BLOOM_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
                // write postings formats
                metaOut.writeVInt(fieldsGroups.size());
                for (FieldsGroup group : fieldsGroups) {
                    group.writeTo(metaOut, state.fieldInfos);
                }
                // Write bloom filters metadata
                metaOut.writeVInt(bloomFilters.size());
                for (BloomFilter bloomFilter : bloomFilters) {
                    bloomFilter.writeTo(metaOut, state.fieldInfos);
                }
                metaOut.writeVLong(indexFileLength);
                CodecUtil.writeFooter(metaOut);
            }
        }
    }

    private record BloomFilter(String field, long startFilePointer, int bloomFilterSize) {
        void writeTo(IndexOutput out, FieldInfos fieldInfos) throws IOException {
            out.writeVInt(fieldInfos.fieldInfo(field).number);
            out.writeVLong(startFilePointer);
            out.writeVInt(bloomFilterSize);
        }

        static BloomFilter readFrom(IndexInput in, FieldInfos fieldInfos) throws IOException {
            final String fieldName = fieldInfos.fieldInfo(in.readVInt()).name;
            final long startFilePointer = in.readVLong();
            final int bloomFilterSize = in.readVInt();
            return new BloomFilter(fieldName, startFilePointer, bloomFilterSize);
        }
    }

    private record FieldsGroup(PostingsFormat postingsFormat, String suffix, List<String> fields) {
        void writeTo(IndexOutput out, FieldInfos fieldInfos) throws IOException {
            out.writeString(postingsFormat.getName());
            out.writeString(suffix);
            out.writeVInt(fields.size());
            for (String field : fields) {
                out.writeVInt(fieldInfos.fieldInfo(field).number);
            }
        }

        static FieldsGroup readFrom(IndexInput in, FieldInfos fieldInfos) throws IOException {
            final PostingsFormat postingsFormat = forName(in.readString());
            final String suffix = in.readString();
            final int numFields = in.readVInt();
            final List<String> fields = new ArrayList<>();
            for (int i = 0; i < numFields; i++) {
                fields.add(fieldInfos.fieldInfo(in.readVInt()).name);
            }
            return new FieldsGroup(postingsFormat, suffix, fields);
        }
    }

    static final class FieldsReader extends FieldsProducer {
        private final Map<String, BloomFilter> bloomFilters;
        private final List<Closeable> toCloses = new ArrayList<>();
        private final Map<String, FieldsProducer> readerMap = new HashMap<>();
        private final IndexInput indexIn;

        FieldsReader(SegmentReadState state) throws IOException {
            boolean success = false;
            try (
                ChecksumIndexInput metaIn = state.directory.openChecksumInput(
                    metaFile(state.segmentInfo, state.segmentSuffix),
                    IOContext.READONCE
                )
            ) {
                Map<String, BloomFilter> bloomFilters = null;
                Throwable priorE = null;
                long indexFileLength = 0;
                try {
                    CodecUtil.checkIndexHeader(
                        metaIn,
                        BLOOM_CODEC_NAME,
                        VERSION_START,
                        VERSION_CURRENT,
                        state.segmentInfo.getId(),
                        state.segmentSuffix
                    );
                    // read postings formats
                    final int numFieldsGroups = metaIn.readVInt();
                    for (int i = 0; i < numFieldsGroups; i++) {
                        final FieldsGroup group = FieldsGroup.readFrom(metaIn, state.fieldInfos);
                        final FieldsProducer reader = group.postingsFormat.fieldsProducer(new SegmentReadState(state, group.suffix));
                        toCloses.add(reader);
                        for (String field : group.fields) {
                            readerMap.put(field, reader);
                        }
                    }
                    // read bloom filters
                    final int numBloomFilters = metaIn.readVInt();
                    bloomFilters = new HashMap<>(numBloomFilters);
                    for (int i = 0; i < numBloomFilters; i++) {
                        final BloomFilter bloomFilter = BloomFilter.readFrom(metaIn, state.fieldInfos);
                        bloomFilters.put(bloomFilter.field, bloomFilter);
                    }

                    indexFileLength = metaIn.readVLong();
                } catch (Throwable t) {
                    priorE = t;
                } finally {
                    CodecUtil.checkFooter(metaIn, priorE);
                }
                this.bloomFilters = bloomFilters;
                indexIn = state.directory.openInput(indexFile(state.segmentInfo, state.segmentSuffix), state.context);
                toCloses.add(indexIn);
                CodecUtil.checkIndexHeader(
                    indexIn,
                    BLOOM_CODEC_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                CodecUtil.retrieveChecksum(indexIn, indexFileLength);
                assert assertBloomFilterSizes(state.segmentInfo);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toCloses);
                }
            }
        }

        private boolean assertBloomFilterSizes(SegmentInfo segmentInfo) {
            for (BloomFilter bloomFilter : bloomFilters.values()) {
                assert bloomFilter.bloomFilterSize == bloomFilterSize(segmentInfo.maxDoc())
                    : "bloom_filter=" + bloomFilter + ", max_docs=" + segmentInfo.maxDoc();
            }
            return true;
        }

        @Override
        public Iterator<String> iterator() {
            return readerMap.keySet().iterator();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(toCloses);
        }

        @Override
        public Terms terms(String field) throws IOException {
            final FieldsProducer reader = readerMap.get(field);
            if (reader == null) {
                return null;
            }
            final Terms terms = reader.terms(field);
            if (terms == null) {
                return null;
            }
            final BloomFilter bloomFilter = bloomFilters.get(field);
            if (bloomFilter != null) {
                final RandomAccessInput data = indexIn.randomAccessSlice(
                    bloomFilter.startFilePointer(),
                    numBytesForBloomFilter(bloomFilter.bloomFilterSize)
                );
                return new BloomFilterTerms(terms, data, bloomFilter.bloomFilterSize);
            } else {
                return terms;
            }
        }

        @Override
        public int size() {
            return readerMap.size();
        }

        @Override
        public void checkIntegrity() throws IOException {
            // already fully checked the meta file; let's fully checked the index file.
            CodecUtil.checksumEntireFile(indexIn);
            // multiple fields can share the same reader
            final Set<FieldsProducer> seenReaders = new HashSet<>();
            for (FieldsProducer reader : readerMap.values()) {
                if (seenReaders.add(reader)) {
                    reader.checkIntegrity();
                }
            }
        }
    }

    private static class BloomFilterTerms extends FilterLeafReader.FilterTerms {
        private final RandomAccessInput data;
        private final int bloomFilterSize;
        private final int[] hashes = new int[NUM_HASH_FUNCTIONS];

        BloomFilterTerms(Terms in, RandomAccessInput data, int bloomFilterSize) {
            super(in);
            this.data = data;
            this.bloomFilterSize = bloomFilterSize;
        }

        private boolean mayContainTerm(BytesRef term) throws IOException {
            hashTerm(term, hashes);
            for (int hash : hashes) {
                hash = hash % bloomFilterSize;
                final int pos = hash >> 3;
                final int mask = 1 << (hash & 7);
                final byte bits = data.readByte(pos);
                if ((bits & mask) == 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new LazyFilterTermsEnum() {
                private TermsEnum delegate;

                @Override
                TermsEnum getDelegate() throws IOException {
                    if (delegate == null) {
                        delegate = in.iterator();
                    }
                    return delegate;
                }

                @Override
                public boolean seekExact(BytesRef term) throws IOException {
                    if (mayContainTerm(term)) {
                        return getDelegate().seekExact(term);
                    } else {
                        return false;
                    }
                }

                @Override
                public void seekExact(BytesRef term, TermState state) throws IOException {
                    getDelegate().seekExact(term, state);
                }

                @Override
                public TermState termState() throws IOException {
                    // TODO: return TermState that includes BloomFilter and fix _disk_usage API
                    return getDelegate().termState();
                }
            };
        }
    }

    private abstract static class LazyFilterTermsEnum extends BaseTermsEnum {
        abstract TermsEnum getDelegate() throws IOException;

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            return getDelegate().seekCeil(text);
        }

        @Override
        public void seekExact(long ord) throws IOException {
            getDelegate().seekExact(ord);
        }

        @Override
        public BytesRef term() throws IOException {
            return getDelegate().term();
        }

        @Override
        public long ord() throws IOException {
            return getDelegate().ord();
        }

        @Override
        public int docFreq() throws IOException {
            return getDelegate().docFreq();
        }

        @Override
        public long totalTermFreq() throws IOException {
            return getDelegate().totalTermFreq();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            return getDelegate().postings(reuse, flags);
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            return getDelegate().impacts(flags);
        }

        @Override
        public BytesRef next() throws IOException {
            return getDelegate().next();
        }

        @Override
        public AttributeSource attributes() {
            try {
                return getDelegate().attributes();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static int bloomFilterSize(int maxDocs) {
        if (maxDocs < 1) {
            throw new IllegalStateException("maxDocs must be greater than or equal to 1, got " + maxDocs);
        }
        // 10% saturation (i.e., 10 bits for each term)
        long numBits = maxDocs * (long) BITS_PER_ENTRY;
        // Round to the next multiple of 8 since we can only store whole bytes
        numBits = ((numBits - 1) | 0x07L) + 1;
        if (numBits > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) numBits;
        }
    }

    static int numBytesForBloomFilter(int bloomFilterSize) {
        return Math.toIntExact((bloomFilterSize + 7L) / 8L);
    }

    // Uses MurmurHash3-128 to generate a 64-bit hash value, then picks 7 subsets of 31 bits each and returns the values in the
    // outputs array. This provides us with 7 reasonably independent hashes of the data for the cost of one MurmurHash3 calculation.
    static int[] hashTerm(BytesRef br, int[] outputs) {
        final long hash64 = MurmurHash3.hash64(br.bytes, br.offset, br.length);
        final int upperHalf = (int) (hash64 >> 32);
        final int lowerHalf = (int) hash64;
        // Derive 7 hash outputs by combining the two 64-bit halves, adding the upper half multiplied with different small constants
        // without common gcd.
        outputs[0] = (lowerHalf + 2 * upperHalf) & 0x7FFF_FFFF;
        outputs[1] = (lowerHalf + 3 * upperHalf) & 0x7FFF_FFFF;
        outputs[2] = (lowerHalf + 5 * upperHalf) & 0x7FFF_FFFF;
        outputs[3] = (lowerHalf + 7 * upperHalf) & 0x7FFF_FFFF;
        outputs[4] = (lowerHalf + 11 * upperHalf) & 0x7FFF_FFFF;
        outputs[5] = (lowerHalf + 13 * upperHalf) & 0x7FFF_FFFF;
        outputs[6] = (lowerHalf + 17 * upperHalf) & 0x7FFF_FFFF;
        return outputs;
    }

    //
    // The following Murmur3 implementation is borrowed from commons-codec.
    //

    /**
     * Implementation of the MurmurHash3 128-bit hash functions.
     *
     * <p>
     * MurmurHash is a non-cryptographic hash function suitable for general hash-based lookup. The name comes from two basic
     * operations, multiply (MU) and rotate (R), used in its inner loop. Unlike cryptographic hash functions, it is not
     * specifically designed to be difficult to reverse by an adversary, making it unsuitable for cryptographic purposes.
     * </p>
     *
     * <p>
     * This contains a Java port of the 32-bit hash function {@code MurmurHash3_x86_32} and the 128-bit hash function
     * {@code MurmurHash3_x64_128} from Austin Appleby's original {@code c++} code in SMHasher.
     * </p>
     *
     * <p>
     * This is public domain code with no copyrights. From home page of
     * <a href="https://github.com/aappleby/smhasher">SMHasher</a>:
     * </p>
     *
     * <blockquote> "All MurmurHash versions are public domain software, and the author disclaims all copyright to their
     * code." </blockquote>
     *
     * <p>
     * Original adaption from Apache Hive. That adaption contains a {@code hash64} method that is not part of the original
     * MurmurHash3 code. It is not recommended to use these methods. They will be removed in a future release. To obtain a
     * 64-bit hash use half of the bits from the {@code hash128x64} methods using the input data converted to bytes.
     * </p>
     *
     * @see <a href="https://en.wikipedia.org/wiki/MurmurHash">MurmurHash</a>
     * @see <a href="https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp"> Original MurmurHash3 c++
     *      code</a>
     * @see <a href=
     *      "https://github.com/apache/hive/blob/master/storage-api/src/java/org/apache/hive/common/util/Murmur3.java">
     *      Apache Hive Murmer3</a>
     * @since 1.13
     */
    public static final class MurmurHash3 {
        /**
         * A default seed to use for the murmur hash algorithm.
         * Has the value {@code 104729}.
         */
        public static final int DEFAULT_SEED = 104729;

        // Constants for 128-bit variant
        private static final long C1 = 0x87c37b91114253d5L;
        private static final long C2 = 0x4cf5ad432745937fL;
        private static final int R1 = 31;
        private static final int R2 = 27;
        private static final int R3 = 33;
        private static final int M = 5;
        private static final int N1 = 0x52dce729;
        private static final int N2 = 0x38495ab5;

        /** No instance methods. */
        private MurmurHash3() {}

        /**
         * Generates 64-bit hash from the byte array with the given offset, length and seed by discarding the second value of the 128-bit
         * hash.
         *
         * This version uses the default seed.
         *
         * @param data The input byte array
         * @param offset The first element of array
         * @param length The length of array
         * @return The sum of the two 64-bit hashes that make up the hash128
         */
        @SuppressWarnings("fallthrough")
        public static long hash64(final byte[] data, final int offset, final int length) {
            long h1 = MurmurHash3.DEFAULT_SEED;
            long h2 = MurmurHash3.DEFAULT_SEED;
            final int nblocks = length >> 4;

            // body
            for (int i = 0; i < nblocks; i++) {
                final int index = offset + (i << 4);
                long k1 = ByteUtils.readLongLE(data, index);
                long k2 = ByteUtils.readLongLE(data, index + 8);

                // mix functions for k1
                k1 *= C1;
                k1 = Long.rotateLeft(k1, R1);
                k1 *= C2;
                h1 ^= k1;
                h1 = Long.rotateLeft(h1, R2);
                h1 += h2;
                h1 = h1 * M + N1;

                // mix functions for k2
                k2 *= C2;
                k2 = Long.rotateLeft(k2, R3);
                k2 *= C1;
                h2 ^= k2;
                h2 = Long.rotateLeft(h2, R1);
                h2 += h1;
                h2 = h2 * M + N2;
            }

            // tail
            long k1 = 0;
            long k2 = 0;
            final int index = offset + (nblocks << 4);
            switch (offset + length - index) {
                case 15:
                    k2 ^= ((long) data[index + 14] & 0xff) << 48;
                case 14:
                    k2 ^= ((long) data[index + 13] & 0xff) << 40;
                case 13:
                    k2 ^= ((long) data[index + 12] & 0xff) << 32;
                case 12:
                    k2 ^= ((long) data[index + 11] & 0xff) << 24;
                case 11:
                    k2 ^= ((long) data[index + 10] & 0xff) << 16;
                case 10:
                    k2 ^= ((long) data[index + 9] & 0xff) << 8;
                case 9:
                    k2 ^= data[index + 8] & 0xff;
                    k2 *= C2;
                    k2 = Long.rotateLeft(k2, R3);
                    k2 *= C1;
                    h2 ^= k2;

                case 8:
                    k1 ^= ((long) data[index + 7] & 0xff) << 56;
                case 7:
                    k1 ^= ((long) data[index + 6] & 0xff) << 48;
                case 6:
                    k1 ^= ((long) data[index + 5] & 0xff) << 40;
                case 5:
                    k1 ^= ((long) data[index + 4] & 0xff) << 32;
                case 4:
                    k1 ^= ((long) data[index + 3] & 0xff) << 24;
                case 3:
                    k1 ^= ((long) data[index + 2] & 0xff) << 16;
                case 2:
                    k1 ^= ((long) data[index + 1] & 0xff) << 8;
                case 1:
                    k1 ^= data[index] & 0xff;
                    k1 *= C1;
                    k1 = Long.rotateLeft(k1, R1);
                    k1 *= C2;
                    h1 ^= k1;
            }

            // finalization
            h1 ^= length;
            h2 ^= length;

            h1 += h2;
            h2 += h1;

            h1 = fmix64(h1);
            h2 = fmix64(h2);

            h1 += h2;

            return h1;
        }

        /**
         * Performs the final avalanche mix step of the 64-bit hash function {@code MurmurHash3_x64_128}.
         *
         * @param hash The current hash
         * @return The final hash
         */
        private static long fmix64(long hash) {
            hash ^= (hash >>> 33);
            hash *= 0xff51afd7ed558ccdL;
            hash ^= (hash >>> 33);
            hash *= 0xc4ceb9fe1a85ec53L;
            hash ^= (hash >>> 33);
            return hash;
        }
    }
}
