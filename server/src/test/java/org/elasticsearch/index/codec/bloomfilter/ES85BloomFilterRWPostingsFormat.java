/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

final class ES85BloomFilterRWPostingsFormat extends ES85BloomFilterPostingsFormat {

    private final Function<String, PostingsFormat> postingsFormats;
    private final BigArrays bigArrays;

    ES85BloomFilterRWPostingsFormat(BigArrays bigArrays, Function<String, PostingsFormat> postingsFormats) {
        super();
        this.bigArrays = Objects.requireNonNull(bigArrays);
        this.postingsFormats = Objects.requireNonNull(postingsFormats);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (postingsFormats == null || bigArrays == null) {
            assert false : BLOOM_CODEC_NAME + " was initialized with a wrong constructor";
            throw new UnsupportedOperationException(BLOOM_CODEC_NAME + " was initialized with a wrong constructor");
        }
        return new FieldsWriter(state);
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
                group.fields().add(field);
            }
            for (FieldsGroup group : currentGroups.values()) {
                final FieldsConsumer writer = group.postingsFormat().fieldsConsumer(new SegmentWriteState(state, group.suffix()));
                toCloses.add(writer);
                final Fields maskedFields = new FilterLeafReader.FilterFields(fields) {
                    @Override
                    public Iterator<String> iterator() {
                        return group.fields().iterator();
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
                IOUtils.close(toCloses);
            }
            try (IndexOutput metaOut = state.directory.createOutput(metaFile(state.segmentInfo, state.segmentSuffix), state.context)) {
                CodecUtil.writeIndexHeader(metaOut, BLOOM_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
                // write postings formats
                metaOut.writeVInt(fieldsGroups.size());
                for (FieldsGroup group : fieldsGroups) {
                    group.writeTo(metaOut, state.fieldInfos);
                }
                // Write bloom filters
                metaOut.writeVInt(bloomFilters.size());
                for (BloomFilter bloomFilter : bloomFilters) {
                    bloomFilter.writeTo(metaOut, state.fieldInfos);
                }
                CodecUtil.writeFooter(metaOut);
            }
        }
    }
}
