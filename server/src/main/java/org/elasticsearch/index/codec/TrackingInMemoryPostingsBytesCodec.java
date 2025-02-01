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
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntConsumer;

public final class TrackingInMemoryPostingsBytesCodec extends FilterCodec {
    public static final String IN_MEMORY_POSTINGS_BYTES_KEY = "es.postings.in_memory_bytes";

    public TrackingInMemoryPostingsBytesCodec(Codec delegate) {
        super(delegate.getName(), delegate);
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        SegmentInfoFormat format = super.segmentInfoFormat();
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return format.read(directory, segmentName, segmentID, context);
            }

            @Override
            public void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException {
                format.write(dir, info, ioContext);
            }
        };
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
        final Map<String, Integer> maxLengths = new HashMap<>();

        TrackingLengthFieldsConsumer(SegmentWriteState state, FieldsConsumer in) {
            this.state = state;
            this.in = in;
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
            in.write(new TrackingLengthFields(fields, maxLengths), norms);
            long totalLength = 0;
            for (int len : maxLengths.values()) {
                totalLength += len; // minTerm
                totalLength += len; // maxTerm
            }
            state.segmentInfo.putAttribute(IN_MEMORY_POSTINGS_BYTES_KEY, Long.toString(totalLength));
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    static final class TrackingLengthFields extends FilterLeafReader.FilterFields {
        final Map<String, Integer> maxLengths;

        TrackingLengthFields(Fields in, Map<String, Integer> maxLengths) {
            super(in);
            this.maxLengths = maxLengths;
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = super.terms(field);
            if (terms == null) {
                return null;
            }
            return new TrackingLengthTerms(terms, len -> maxLengths.compute(field, (k, v) -> v == null ? len : Math.max(v, len)));
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
        final IntConsumer onFinish;

        TrackingLengthTermsEnum(TermsEnum in, IntConsumer onFinish) {
            super(in);
            this.onFinish = onFinish;
        }

        @Override
        public BytesRef next() throws IOException {
            final BytesRef term = super.next();
            if (term != null) {
                maxTermLength = Math.max(maxTermLength, term.length);
            } else {
                onFinish.accept(maxTermLength);
            }
            return term;
        }
    }
}
