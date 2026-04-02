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
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.FieldInfos;
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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
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
import java.util.Set;

/**
 * This implementation is forked from Lucene's BloomFilterPosting to support on-disk bloom filters.
 * <p>
 * A {@link PostingsFormat} useful for low doc-frequency fields such as primary keys. Bloom filters
 * offers "fast-fail" for reads in segments known to have no record of the key.
 */
public class ES85BloomFilterPostingsFormat extends PostingsFormat {
    static final String BLOOM_CODEC_NAME = "ES85BloomFilter";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;
    static final String BLOOM_FILTER_META_FILE = "bfm";
    static final String BLOOM_FILTER_INDEX_FILE = "bfi";

    public ES85BloomFilterPostingsFormat() {
        super(BLOOM_CODEC_NAME);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FieldsReader(state);
    }

    @Override
    public String toString() {
        return BLOOM_CODEC_NAME;
    }

    static String metaFile(SegmentInfo si, String segmentSuffix) {
        return IndexFileNames.segmentFileName(si.name, segmentSuffix, BLOOM_FILTER_META_FILE);
    }

    static String indexFile(SegmentInfo si, String segmentSuffix) {
        return IndexFileNames.segmentFileName(si.name, segmentSuffix, BLOOM_FILTER_INDEX_FILE);
    }

    record BloomFilter(String field, long startFilePointer, int bloomFilterSize) {
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

    record FieldsGroup(PostingsFormat postingsFormat, String suffix, List<String> fields) {
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
            try (ChecksumIndexInput metaIn = state.directory.openChecksumInput(metaFile(state.segmentInfo, state.segmentSuffix))) {
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

        BloomFilterTerms(Terms in, RandomAccessInput data, int bloomFilterSize) {
            super(in);
            this.data = data;
            this.bloomFilterSize = bloomFilterSize;
        }

        private boolean mayContainTerm(BytesRef term) throws IOException {
            final int hash = hashTerm(term) % bloomFilterSize;
            final int pos = hash >> 3;
            final int mask = 1 << (hash & 0x7);
            final byte bits = data.readByte(pos);
            return (bits & mask) != 0;
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
        // 10% saturation (i.e., 10 bits for each term)
        final long numBits = maxDocs * 10L;
        if (numBits > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) numBits;
        }
    }

    static int numBytesForBloomFilter(int bloomFilterSize) {
        return Math.toIntExact((bloomFilterSize + 7L) / 8L);
    }

    static int hashTerm(BytesRef br) {
        final int hash = murmurhash3_x86_32(br.bytes, br.offset, br.length, 0x9747b28c);
        return hash & 0x7FFF_FFFF;
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
}
