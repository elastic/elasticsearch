/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.function.IntConsumer;

/**
 * A codec that tracks the length of the min and max written terms. Used to improve memory usage estimates in serverless, since
 * {@link org.apache.lucene.codecs.lucene103.blocktree.FieldReader} keeps an in-memory reference to the min and max term.
 */
public class TrackingPostingsInMemoryBytesCodec extends FilterCodec {
    public static final String IN_MEMORY_POSTINGS_BYTES_KEY = "es.postings.in_memory_bytes";

    public TrackingPostingsInMemoryBytesCodec(Codec delegate) {
        super(delegate.getName(), delegate);
    }

    @Override
    public PostingsFormat postingsFormat() {
        PostingsFormat format = super.postingsFormat();

        return new PostingsFormat(format.getName()) {
            @Override
            public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                FieldsConsumer consumer = format.fieldsConsumer(state);
                return new TrackingLengthFieldsConsumer(state, consumer);
            }

            @Override
            public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
                return format.fieldsProducer(state);
            }
        };
    }

    static final class TrackingLengthFieldsConsumer extends FieldsConsumer {
        final SegmentWriteState state;
        final FieldsConsumer in;
        final IntHashSet seenFields;
        final long[] totalBytes;

        TrackingLengthFieldsConsumer(SegmentWriteState state, FieldsConsumer in) {
            this.state = state;
            this.in = in;
            this.totalBytes = new long[1];
            // Alternatively, we can consider using a FixedBitSet here and size to max(fieldNumber).
            // This should be faster without worrying too much about memory usage.
            this.seenFields = new IntHashSet(state.fieldInfos.size());
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
            in.write(new TrackingLengthFields(fields, state.fieldInfos, seenFields, totalBytes), norms);
            state.segmentInfo.putAttribute(IN_MEMORY_POSTINGS_BYTES_KEY, Long.toString(totalBytes[0]));
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    static final class TrackingLengthFields extends FilterLeafReader.FilterFields {
        final FieldInfos fieldInfos;
        final IntHashSet seenFields;
        final long[] totalBytes;

        TrackingLengthFields(Fields in, FieldInfos fieldInfos, IntHashSet seenFields, long[] totalBytes) {
            super(in);
            this.seenFields = seenFields;
            this.fieldInfos = fieldInfos;
            this.totalBytes = totalBytes;
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = super.terms(field);
            if (terms == null) {
                return null;
            }
            int fieldNum = fieldInfos.fieldInfo(field).number;
            if (seenFields.add(fieldNum)) {
                return new TrackingLengthTerms(terms, bytes -> totalBytes[0] += bytes);
            } else {
                // As far as I know only when bloom filter for _id filter gets written this method gets invoked twice for the same field.
                // So maybe we can get rid of the seenFields here? And just keep track of whether _id field has been seen? However, this
                // is fragile and could make us vulnerable to tricky bugs in the future if this is no longer the case.
                return terms;
            }
        }
    }

    static final class TrackingLengthTerms extends FilterLeafReader.FilterTerms {
        final IntConsumer onFinish;

        TrackingLengthTerms(Terms in, IntConsumer onFinish) {
            super(in);
            this.onFinish = onFinish;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new TrackingLengthTermsEnum(super.iterator(), onFinish);
        }
    }

    static final class TrackingLengthTermsEnum extends FilterLeafReader.FilterTermsEnum {
        int maxTermLength = 0;
        int minTermLength = 0;
        int termCount = 0;
        final IntConsumer onFinish;

        TrackingLengthTermsEnum(TermsEnum in, IntConsumer onFinish) {
            super(in);
            this.onFinish = onFinish;
        }

        @Override
        public BytesRef next() throws IOException {
            final BytesRef term = super.next();
            if (term != null) {
                if (termCount == 0) {
                    minTermLength = term.length;
                }
                maxTermLength = term.length;
                termCount++;
            } else {
                if (termCount == 1) {
                    // If the minTerm and maxTerm are the same, only one instance is kept on the heap.
                    assert minTermLength == maxTermLength;
                    onFinish.accept(maxTermLength);
                } else {
                    onFinish.accept(maxTermLength + minTermLength);
                }
            }
            return term;
        }
    }
}
