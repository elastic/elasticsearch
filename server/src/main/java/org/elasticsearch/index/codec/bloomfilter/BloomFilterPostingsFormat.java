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
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.BaseTermsEnum;
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
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This implementation is forked from Lucene's BloomFilterPosting to support on-disk bloom filters.
 * <p>
 * A {@link PostingsFormat} useful for low doc-frequency fields such as primary keys. Bloom filters
 * offers "fast-fail" for reads in segments known to have no record of the key.
 */
public class BloomFilterPostingsFormat extends PostingsFormat {
    static final String BLOOM_CODEC_NAME = "ES84BloomFilter";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;
    static final String BLOOM_FILTER_META_FILE = "bfm";
    static final String BLOOM_FILTER_INDEX_FILE = "bfi";

    private PostingsFormat postingsFormat;
    private BigArrays bigArrays;

    public BloomFilterPostingsFormat(PostingsFormat postingsFormat, BigArrays bigArrays) {
        this();
        this.postingsFormat = Objects.requireNonNull(postingsFormat);
        this.bigArrays = Objects.requireNonNull(bigArrays);
        if (postingsFormat instanceof PerFieldPostingsFormat) {
            assert false : "Can't use PerFieldPostingsFormat with ESBloomFilterPostingsFormat";
            throw new UnsupportedOperationException("Can't use PerFieldPostingsFormat with ESBloomFilterPostingsFormat");
        }
    }

    public BloomFilterPostingsFormat() {
        super(BLOOM_CODEC_NAME);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (postingsFormat == null || bigArrays == null) {
            assert false : "ESBloomFilterPostingsFormat was initialized with a wrong constructor";
            throw new UnsupportedOperationException("ESBloomFilterPostingsFormat was initialized with a wrong constructor");
        }
        return new FieldsWriter(postingsFormat, state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FieldsReader(state);
    }

    @Override
    public String toString() {
        return "ESBloomFilterPostingsFormat(" + postingsFormat.getName() + ")";
    }

    private static String metaField(SegmentInfo si, String segmentSuffix) {
        return IndexFileNames.segmentFileName(si.name, segmentSuffix, BLOOM_FILTER_META_FILE);
    }

    private static String indexField(SegmentInfo si, String segmentSuffix) {
        return IndexFileNames.segmentFileName(si.name, segmentSuffix, BLOOM_FILTER_INDEX_FILE);
    }

    final class FieldsWriter extends FieldsConsumer {
        private final FieldsConsumer termsWriter;
        private final SegmentWriteState state;
        private final IndexOutput indexOut;
        private final List<BloomFilter> bloomFilters = new ArrayList<>();
        private boolean closed;

        FieldsWriter(PostingsFormat postingsFormat, SegmentWriteState state) throws IOException {
            this.state = state;
            final List<Closeable> toCloses = new ArrayList<>(2);
            try {
                termsWriter = postingsFormat.fieldsConsumer(state);
                toCloses.add(termsWriter);
                indexOut = state.directory.createOutput(indexField(state.segmentInfo, state.segmentSuffix), state.context);
                toCloses.add(indexOut);
                CodecUtil.writeIndexHeader(indexOut, BLOOM_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
                toCloses.clear();
            } finally {
                IOUtils.closeWhileHandlingException(toCloses);
            }
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
            termsWriter.write(fields, norms);
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
                        // Do we need to skip terms without postings?
                        final int hash = hashTerm(term) % bloomFilterSize;
                        final int pos = hash >> 3;
                        final int mask = 1 << (hash & 0x7);
                        final byte val = (byte) (buffer.get(pos) | mask);
                        buffer.set(pos, val);
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
                IOUtils.close(termsWriter, indexOut);
            }
            try (IndexOutput metaOut = state.directory.createOutput(metaField(state.segmentInfo, state.segmentSuffix), state.context)) {
                CodecUtil.writeIndexHeader(metaOut, BLOOM_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
                metaOut.writeString(postingsFormat.getName());
                metaOut.writeVInt(bloomFilters.size());
                for (BloomFilter field : bloomFilters) {
                    metaOut.writeVInt(state.fieldInfos.fieldInfo(field.field).number);
                    metaOut.writeVLong(field.startFilePointer);
                    metaOut.writeVInt(field.bloomFilterSize);
                }
                CodecUtil.writeFooter(metaOut);
            }
        }
    }

    private record BloomFilter(String field, long startFilePointer, int bloomFilterSize) {

    }

    static final class FieldsReader extends FieldsProducer {
        private final FieldsProducer termsReader;
        private final Map<String, BloomFilter> bloomFilters;
        private final IndexInput indexIn;

        FieldsReader(SegmentReadState state) throws IOException {
            final List<Closeable> toCloses = new ArrayList<>(2);
            try (
                ChecksumIndexInput metaIn = state.directory.openChecksumInput(
                    metaField(state.segmentInfo, state.segmentSuffix),
                    IOContext.READONCE
                )
            ) {
                CodecUtil.checkIndexHeader(
                    metaIn,
                    BLOOM_CODEC_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                final PostingsFormat postingsFormat = forName(metaIn.readString());
                final int numBloomFilters = metaIn.readVInt();
                bloomFilters = new HashMap<>(numBloomFilters);
                for (int i = 0; i < numBloomFilters; i++) {
                    final int fieldNumber = metaIn.readVInt();
                    final long startFilePointer = metaIn.readVLong();
                    final int bloomFilterSize = metaIn.readVInt();
                    assert bloomFilterSize == bloomFilterSize(state.segmentInfo.maxDoc())
                        : "bloom_filter_size=" + bloomFilterSize + ", max_docs=" + state.segmentInfo.maxDoc();
                    final String fieldName = state.fieldInfos.fieldInfo(fieldNumber).name;
                    bloomFilters.put(fieldName, new BloomFilter(fieldName, startFilePointer, bloomFilterSize));
                }
                CodecUtil.checkFooter(metaIn);
                termsReader = postingsFormat.fieldsProducer(state);
                toCloses.add(this.termsReader);
                indexIn = state.directory.openInput(indexField(state.segmentInfo, state.segmentSuffix), state.context);
                toCloses.add(indexIn);
                CodecUtil.checkIndexHeader(
                    indexIn,
                    BLOOM_CODEC_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                toCloses.clear();
            } finally {
                IOUtils.closeWhileHandlingException(toCloses);
            }
        }

        @Override
        public Iterator<String> iterator() {
            return termsReader.iterator();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(termsReader, indexIn);
        }

        @Override
        public Terms terms(String field) throws IOException {
            final Terms terms = termsReader.terms(field);
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
            return termsReader.size();
        }

        @Override
        public void checkIntegrity() throws IOException {
            termsReader.checkIntegrity();
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
        return hashTerm(br.bytes, 0x9747b28c, br.offset, br.length);
    }

    /**
     * This is a very fast, non-cryptographic hash suitable for general hash-based lookup.
     * <p>
     * The C version of MurmurHash 2.0 found at that site was ported to Java by Andrzej Bialecki (ab at getopt org).
     * <p>
     * The code from getopt.org was adapted by Mark Harwood in the form here as one of a pluggable
     * choice of hashing functions as the core function had to be adapted to work with BytesRefs with
     * offsets and lengths rather than raw byte arrays.
     */
    private static int hashTerm(byte[] data, int seed, int offset, int len) {
        final int m = 0x5bd1e995;
        final int r = 24;
        int h = seed ^ len;
        final int len_4 = len >> 2;
        for (int i = 0; i < len_4; i++) {
            int i_4 = offset + (i << 2);
            int k = data[i_4 + 3];
            k = k << 8;
            k = k | (data[i_4 + 2] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 1] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 0] & 0xff);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        final int len_m = len_4 << 2;
        final int left = len - len_m;
        if (left != 0) {
            if (left >= 3) {
                h ^= data[offset + len - 3] << 16;
            }
            if (left >= 2) {
                h ^= data[offset + len - 2] << 8;
            }
            if (left >= 1) {
                h ^= data[offset + len - 1];
            }
            h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h < 0 ? -h : h;
    }
}
