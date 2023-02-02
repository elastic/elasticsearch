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
 * Modifications copyright (C) 2022 Elasticsearch B.V.
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
import org.apache.lucene.util.BitUtil;
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
        private final int[] hashes = new int[7];

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
            for (String field : fields) {
                final Terms terms = fields.terms(field);
                if (terms == null) {
                    continue;
                }
                final int bloomFilterSize = bloomFilterSize(state.segmentInfo.maxDoc());
                final int numBytes = numBytesForBloomFilter(bloomFilterSize);
                try (ByteArray buffer = bigArrays.newByteArray(numBytes)) {
                    final TermsEnum termsEnum = terms.iterator();
                    while (true) {
                        final BytesRef term = termsEnum.next();
                        if (term == null) {
                            break;
                        }

                        hashTerm(term, hashes);
                        for (int hash : hashes) {
                            hash = hash % bloomFilterSize;
                            final int pos = hash >> 3;
                            final int mask = 1 << (hash & 7);
                            final byte val = (byte) (buffer.get(pos) | mask);
                            buffer.set(pos, val);
                        }
                    }
                    bloomFilters.add(new BloomFilter(field, indexOut.getFilePointer(), bloomFilterSize));
                    final BytesReference bytes = BytesReference.fromByteArray(buffer, numBytes);
                    bytes.writeTo(new IndexOutputOutputStream(indexOut));
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                CodecUtil.writeFooter(indexOut);
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
        private final int version;

        FieldsReader(SegmentReadState state) throws IOException {
            boolean success = false;
            try (
                ChecksumIndexInput metaIn = state.directory.openChecksumInput(
                    metaFile(state.segmentInfo, state.segmentSuffix),
                    IOContext.READONCE
                )
            ) {
                this.version = CodecUtil.checkIndexHeader(
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
                CodecUtil.checkFooter(metaIn);
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
                CodecUtil.retrieveChecksum(indexIn);
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
        private final int[] hashes = new int[7];

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
        long numBits = maxDocs * 10L;
        // Round to the next number of 8 since we can only store whole bytes
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

    // Uses CityHash64 to generate a 64-bit hash value, then picks 7 subsets of 31 bits each and returns the values in the
    // outputs array. This provides us with 7 reasonably independent hashes of the data for the cost of one CityHash64 calculation.
    static int[] hashTerm(BytesRef br, int[] outputs) {
        final long hash_v2 = CityHash.cityHash64(br.bytes, br.offset, br.length);
        final int upper_half = (int) hash_v2 >> 32;
        final int lower_half = (int) hash_v2;
        // Derive 7 hash outputs by combining the two 64-bit halves, adding the upper half multiplied with different small constants
        // without common gcd.
        outputs[0] = (lower_half + 2 * upper_half) & 0x7FFF_FFFF;
        outputs[1] = (lower_half + 3 * upper_half) & 0x7FFF_FFFF;
        outputs[2] = (lower_half + 5 * upper_half) & 0x7FFF_FFFF;
        outputs[3] = (lower_half + 7 * upper_half) & 0x7FFF_FFFF;
        outputs[4] = (lower_half + 11 * upper_half) & 0x7FFF_FFFF;
        outputs[5] = (lower_half + 13 * upper_half) & 0x7FFF_FFFF;
        outputs[6] = (lower_half + 17 * upper_half) & 0x7FFF_FFFF;
        return outputs;
    }

    /**
     * Forked from Lucene's StringHelper#murmurhash3_x86_32 so that changes to the Lucene implementation
     * do not break the compatibility of this format.
     */
    @SuppressWarnings("fallthrough")
    private static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc); // round down to 4 byte block

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (int) BitUtil.VH_LE_INT.get(data, i);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (len & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= (data[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= len;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    // A self-contained implementation of CityHash that is used for the Bloom
    // filter.
    /*
     * Copyright (C) 2012 tamtam180
     *
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *
     * http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

    /**
     * @author tamtam180 - kirscheless at gmail.com
     * see http://google-opensource.blogspot.jp/2011/04/introducing-cityhash.html
     * see http://code.google.com/p/cityhash/
     */
    @SuppressWarnings("fallthrough")
    public static class CityHash {

        private static final long k0 = 0xc3a5c85c97cb3127L;
        private static final long k1 = 0xb492b66fbe98f273L;
        private static final long k2 = 0x9ae16a3b2f90404fL;
        private static final long k3 = 0xc949d7c7509e6557L;

        private static long fetch64(byte[] s, int pos) {
            return ByteUtils.readLongLE(s, pos);
        }

        private static int fetch32(byte[] s, int pos) {
            return ByteUtils.readIntLE(s, pos);
        }

        private static long rotate(long val, int shift) {
            return Long.rotateRight(val, shift);
        }

        private static long shiftMix(long val) {
            return val ^ (val >>> 47);
        }

        private static final long kMul = 0x9ddfea08eb382d69L;

        private static long hash128to64(long u, long v) {
            long a = (u ^ v) * kMul;
            a ^= (a >>> 47);
            long b = (v ^ a) * kMul;
            b ^= (b >>> 47);
            b *= kMul;
            return b;
        }

        private static long hashLen16(long u, long v) {
            return hash128to64(u, v);
        }

        private static long hashLen0to16(byte[] s, int pos, int len) {
            if (len > 8) {
                long a = fetch64(s, pos + 0);
                long b = fetch64(s, pos + len - 8);
                return hashLen16(a, rotate(b + len, len)) ^ b;
            }
            if (len >= 4) {
                long a = 0xffffffffL & fetch32(s, pos + 0);
                return hashLen16((a << 3) + len, 0xffffffffL & fetch32(s, pos + len - 4));
            }
            if (len > 0) {
                int a = s[pos + 0] & 0xFF;
                int b = s[pos + (len >>> 1)] & 0xFF;
                int c = s[pos + len - 1] & 0xFF;
                int y = a + (b << 8);
                int z = len + (c << 2);
                return shiftMix(y * k2 ^ z * k3) * k2;
            }
            return k2;
        }

        private static long hashLen17to32(byte[] s, int pos, int len) {
            long a = fetch64(s, pos + 0) * k1;
            long b = fetch64(s, pos + 8);
            long c = fetch64(s, pos + len - 8) * k2;
            long d = fetch64(s, pos + len - 16) * k0;
            return hashLen16(rotate(a - b, 43) + rotate(c, 30) + d, a + rotate(b ^ k3, 20) - c + len);
        }

        private static long[] weakHashLen32WithSeeds(long w, long x, long y, long z, long a, long b) {

            a += w;
            b = rotate(b + a + z, 21);
            long c = a;
            a += x;
            a += y;
            b += rotate(a, 44);
            return new long[] { a + z, b + c };
        }

        private static long[] weakHashLen32WithSeeds(byte[] s, int pos, long a, long b) {
            return weakHashLen32WithSeeds(fetch64(s, pos + 0), fetch64(s, pos + 8), fetch64(s, pos + 16), fetch64(s, pos + 24), a, b);
        }

        private static long hashLen33to64(byte[] s, int pos, int len) {
            long z = fetch64(s, pos + 24);
            long a = fetch64(s, pos + 0) + (fetch64(s, pos + len - 16) + len) * k0;
            long b = rotate(a + z, 52);
            long c = rotate(a, 37);

            a += fetch64(s, pos + 8);
            c += rotate(a, 7);
            a += fetch64(s, pos + 16);

            long vf = a + z;
            long vs = b + rotate(a, 31) + c;

            a = fetch64(s, pos + 16) + fetch64(s, pos + len - 32);
            z = fetch64(s, pos + len - 8);
            b = rotate(a + z, 52);
            c = rotate(a, 37);
            a += fetch64(s, pos + len - 24);
            c += rotate(a, 7);
            a += fetch64(s, pos + len - 16);

            long wf = a + z;
            long ws = b + rotate(a, 31) + c;
            long r = shiftMix((vf + ws) * k2 + (wf + vs) * k0);

            return shiftMix(r * k0 + vs) * k2;

        }

        public static long cityHash64(byte[] s, int pos, int len) {

            if (len <= 32) {
                if (len <= 16) {
                    return hashLen0to16(s, pos, len);
                } else {
                    return hashLen17to32(s, pos, len);
                }
            } else if (len <= 64) {
                return hashLen33to64(s, pos, len);
            }

            long x = fetch64(s, pos + len - 40);
            long y = fetch64(s, pos + len - 16) + fetch64(s, pos + len - 56);
            long z = hashLen16(fetch64(s, pos + len - 48) + len, fetch64(s, pos + len - 24));

            long[] v = weakHashLen32WithSeeds(s, pos + len - 64, len, z);
            long[] w = weakHashLen32WithSeeds(s, pos + len - 32, y + k1, x);
            x = x * k1 + fetch64(s, pos + 0);

            len = (len - 1) & (~63);
            do {
                x = rotate(x + y + v[0] + fetch64(s, pos + 8), 37) * k1;
                y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * k1;
                x ^= w[1];
                y += v[0] + fetch64(s, pos + 40);
                z = rotate(z + w[0], 33) * k1;
                v = weakHashLen32WithSeeds(s, pos + 0, v[1] * k1, x + w[0]);
                w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y + fetch64(s, pos + 16));
                {
                    long swap = z;
                    z = x;
                    x = swap;
                }
                pos += 64;
                len -= 64;
            } while (len != 0);

            return hashLen16(hashLen16(v[0], w[0]) + shiftMix(y) * k1 + z, hashLen16(v[1], w[1]) + x);

        }

        public static long cityHash64WithSeed(byte[] s, int pos, int len, long seed) {
            return cityHash64WithSeeds(s, pos, len, k2, seed);
        }

        public static long cityHash64WithSeeds(byte[] s, int pos, int len, long seed0, long seed1) {
            return hashLen16(cityHash64(s, pos, len) - seed0, seed1);
        }

        public static long[] cityMurmur(byte[] s, int pos, int len, long seed0, long seed1) {

            long a = seed0;
            long b = seed1;
            long c = 0;
            long d = 0;

            int l = len - 16;
            if (l <= 0) {
                a = shiftMix(a * k1) * k1;
                c = b * k1 + hashLen0to16(s, pos, len);
                d = shiftMix(a + (len >= 8 ? fetch64(s, pos + 0) : c));
            } else {

                c = hashLen16(fetch64(s, pos + len - 8) + k1, a);
                d = hashLen16(b + len, c + fetch64(s, pos + len - 16));
                a += d;

                do {
                    a ^= shiftMix(fetch64(s, pos + 0) * k1) * k1;
                    a *= k1;
                    b ^= a;
                    c ^= shiftMix(fetch64(s, pos + 8) * k1) * k1;
                    c *= k1;
                    d ^= c;
                    pos += 16;
                    l -= 16;
                } while (l > 0);
            }

            a = hashLen16(a, c);
            b = hashLen16(d, b);

            return new long[] { a ^ b, hashLen16(b, a) };

        }

        public static long[] cityHash128WithSeed(byte[] s, int pos, int len, long seed0, long seed1) {

            if (len < 128) {
                return cityMurmur(s, pos, len, seed0, seed1);
            }

            long[] v = new long[2], w = new long[2];
            long x = seed0;
            long y = seed1;
            long z = k1 * len;

            v[0] = rotate(y ^ k1, 49) * k1 + fetch64(s, pos);
            v[1] = rotate(v[0], 42) * k1 + fetch64(s, pos + 8);
            w[0] = rotate(y + z, 35) * k1 + x;
            w[1] = rotate(x + fetch64(s, pos + 88), 53) * k1;

            do {
                x = rotate(x + y + v[0] + fetch64(s, pos + 8), 37) * k1;
                y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * k1;

                x ^= w[1];
                y += v[0] + fetch64(s, pos + 40);
                z = rotate(z + w[0], 33) * k1;
                v = weakHashLen32WithSeeds(s, pos + 0, v[1] * k1, x + w[0]);
                w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y + fetch64(s, pos + 16));
                {
                    long swap = z;
                    z = x;
                    x = swap;
                }
                pos += 64;
                x = rotate(x + y + v[0] + fetch64(s, pos + 8), 37) * k1;
                y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * k1;
                x ^= w[1];
                y += v[0] + fetch64(s, pos + 40);
                z = rotate(z + w[0], 33) * k1;
                v = weakHashLen32WithSeeds(s, pos, v[1] * k1, x + w[0]);
                w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y + fetch64(s, pos + 16));
                {
                    long swap = z;
                    z = x;
                    x = swap;
                }
                pos += 64;
                len -= 128;
            } while (len >= 128);

            x += rotate(v[0] + z, 49) * k0;
            z += rotate(w[0], 37) * k0;

            for (int tail_done = 0; tail_done < len;) {
                tail_done += 32;
                y = rotate(x + y, 42) * k0 + v[1];
                w[0] += fetch64(s, pos + len - tail_done + 16);
                x = x * k0 + w[0];
                z += w[1] + fetch64(s, pos + len - tail_done);
                w[1] += v[0];
                v = weakHashLen32WithSeeds(s, pos + len - tail_done, v[0] + z, v[1]);
            }

            x = hashLen16(x, v[0]);
            y = hashLen16(y + z, w[0]);

            return new long[] { hashLen16(x + v[1], w[1]) + y, hashLen16(x + w[1], y + v[1]) };

        }

        public static long[] cityHash128(byte[] s, int pos, int len) {

            if (len >= 16) {
                return cityHash128WithSeed(s, pos + 16, len - 16, fetch64(s, pos + 0) ^ k3, fetch64(s, pos + 8));
            } else if (len >= 8) {
                return cityHash128WithSeed(new byte[0], 0, 0, fetch64(s, pos + 0) ^ (len * k0), fetch64(s, pos + len - 8) ^ k1);
            } else {
                return cityHash128WithSeed(s, pos, len, k0, k1);
            }
        }
    }

    /*
     * Licensed to the Apache Software Foundation (ASF) under one or more
     * contributor license agreements.  See the NOTICE file distributed with
     * this work for additional information regarding copyright ownership.
     * The ASF licenses this file to You under the Apache License, Version 2.0
     * (the "License"); you may not use this file except in compliance with
     * the License.  You may obtain a copy of the License at
     *
     *      http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

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
    public final class MurmurHash3 {
        /**
         * A random number to use for a hash code.
         *
         * @deprecated This is not used internally and will be removed in a future release.
         */
        @Deprecated
        public static final long NULL_HASHCODE = 2862933555777941757L;

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
         * Generates 128-bit hash from the byte array with the given offset, length and seed.
         *
         * <p>This is an implementation of the 128-bit hash function {@code MurmurHash3_x64_128}
         * from Austin Appleby's original MurmurHash3 {@code c++} code in SMHasher.</p>
         *
         * This version uses the default seed.
         *
         * @param data The input byte array
         * @param offset The first element of array
         * @param length The length of array
         * @return The 128-bit hash (2 longs)
         * @deprecated Use {@link #hash128x64(byte[], int, int, int)}. This corrects the seed initialization.
         */
        @Deprecated
        public static long[] hash128(final byte[] data, final int offset, final int length) {
            // ************
            // Note: This deliberately fails to apply masking using 0xffffffffL to the seed
            // to maintain behavioral compatibility with the original version.
            // The implicit conversion to a long will extend a negative sign
            // bit through the upper 32-bits of the long seed. These should be zero.
            // ************
            return hash128x64Internal(data, offset, length, DEFAULT_SEED);
        }

        /**
         * Generates 128-bit hash from the byte array with the given offset, length and seed.
         *
         * <p>This is an implementation of the 128-bit hash function {@code MurmurHash3_x64_128}
         * from Austin Appleby's original MurmurHash3 {@code c++} code in SMHasher.</p>
         *
         * @param data The input byte array
         * @param offset The first element of array
         * @param length The length of array
         * @param seed The initial seed value
         * @return The 128-bit hash (2 longs)
         * @since 1.14
         */
        public static long[] hash128x64(final byte[] data, final int offset, final int length, final int seed) {
            // Use an unsigned 32-bit integer as the seed
            return hash128x64Internal(data, offset, length, seed & 0xffffffffL);
        }

        /**
         * Generates 128-bit hash from the byte array with the given offset, length and seed.
         *
         * <p>This is an implementation of the 128-bit hash function {@code MurmurHash3_x64_128}
         * from Austin Appleby's original MurmurHash3 {@code c++} code in SMHasher.</p>
         *
         * @param data The input byte array
         * @param offset The first element of array
         * @param length The length of array
         * @param seed The initial seed value
         * @return The 128-bit hash (2 longs)
         */
        @SuppressWarnings("fallthrough")
        private static long[] hash128x64Internal(final byte[] data, final int offset, final int length, final long seed) {
            long h1 = seed;
            long h2 = seed;
            final int nblocks = length >> 4;

            // body
            for (int i = 0; i < nblocks; i++) {
                final int index = offset + (i << 4);
                long k1 = getLittleEndianLong(data, index);
                long k2 = getLittleEndianLong(data, index + 8);

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
            h2 += h1;

            return new long[] { h1, h2 };
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

        /**
         * Gets the little-endian long from 8 bytes starting at the specified index.
         *
         * @param data The data
         * @param index The index
         * @return The little-endian long
         */
        private static long getLittleEndianLong(final byte[] data, final int index) {
            return ByteUtils.readLongLE(data, index);
        }
    }
}
