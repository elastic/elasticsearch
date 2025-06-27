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
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.FeatureFlag;

import java.io.IOException;
import java.util.function.IntConsumer;

/**
 * A codec that tracks the length of the min and max written terms. Used to improve memory usage estimates in serverless, since
 * {@link org.apache.lucene.codecs.lucene90.blocktree.FieldReader} keeps an in-memory reference to the min and max term.
 */
public class TrackingPostingsInMemoryBytesCodec extends FilterCodec {
    public static final FeatureFlag TRACK_POSTINGS_IN_MEMORY_BYTES = new FeatureFlag("track_postings_in_memory_bytes");

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
        final IntIntHashMap termsBytesPerField;

        TrackingLengthFieldsConsumer(SegmentWriteState state, FieldsConsumer in) {
            this.state = state;
            this.in = in;
            this.termsBytesPerField = new IntIntHashMap(state.fieldInfos.size());
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
            in.write(new TrackingLengthFields(fields, termsBytesPerField, state.fieldInfos), norms);
            long totalBytes = 0;
            for (int bytes : termsBytesPerField.values) {
                totalBytes += bytes;
            }
            state.segmentInfo.putAttribute(IN_MEMORY_POSTINGS_BYTES_KEY, Long.toString(totalBytes));
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    static final class TrackingLengthFields extends FilterLeafReader.FilterFields {
        final IntIntHashMap termsBytesPerField;
        final FieldInfos fieldInfos;

        TrackingLengthFields(Fields in, IntIntHashMap termsBytesPerField, FieldInfos fieldInfos) {
            super(in);
            this.termsBytesPerField = termsBytesPerField;
            this.fieldInfos = fieldInfos;
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = super.terms(field);
            if (terms == null) {
                return null;
            }
            int fieldNum = fieldInfos.fieldInfo(field).number;
            return new TrackingLengthTerms(
                terms,
                bytes -> termsBytesPerField.put(fieldNum, Math.max(termsBytesPerField.getOrDefault(fieldNum, 0), bytes))
            );
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
