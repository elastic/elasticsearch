/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Thin delegating Lucene format wrappers that isolate a single field's data into its own set of segment files.
 * <p>
 * Lucene's per-field formats ({@code PerFieldPostingsFormat}, the doc-values {@code XPerFieldDocValuesFormat} fork and
 * {@code PerFieldKnnVectorsFormat}) group fields onto a shared file set by keying their internal consumer map on the
 * format <em>instance</em> (object identity) while bumping the file suffix per format <em>name</em>. By handing out a
 * distinct wrapper instance per field — each reporting the delegate's registered format name — every field
 * gets its own suffix, and therefore its own files, while remaining fully readable: on the read path the per-field
 * readers resolve the format by name via SPI, so the wrapper is only ever an indirection at write time.
 * <p>
 * The wrappers do not register themselves with the codec SPI; they are never looked up by name, only delegated through.
 *
 * @see PerFieldFormatSupplier
 */
final class IsolatedFieldFormats {

    private IsolatedFieldFormats() {}

    /** Returns the underlying delegate format if {@code format} is an isolation wrapper, otherwise {@code format} itself. */
    static PostingsFormat unwrap(PostingsFormat format) {
        return format instanceof Postings wrapper ? wrapper.delegate : format;
    }

    /** Returns the underlying delegate format if {@code format} is an isolation wrapper, otherwise {@code format} itself. */
    static DocValuesFormat unwrap(DocValuesFormat format) {
        return format instanceof DocValues wrapper ? wrapper.delegate : format;
    }

    /** Returns the underlying delegate format if {@code format} is an isolation wrapper, otherwise {@code format} itself. */
    static KnnVectorsFormat unwrap(KnnVectorsFormat format) {
        return format instanceof KnnVectors wrapper ? wrapper.delegate : format;
    }

    static final class DocValues extends DocValuesFormat {
        private final DocValuesFormat delegate;

        DocValues(DocValuesFormat delegate) {
            super(delegate.getName());
            this.delegate = delegate;
        }

        @Override
        public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return delegate.fieldsConsumer(state);
        }

        @Override
        public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
            return delegate.fieldsProducer(state);
        }
    }

    static final class Postings extends PostingsFormat {
        private final PostingsFormat delegate;

        Postings(PostingsFormat delegate) {
            super(delegate.getName());
            this.delegate = delegate;
        }

        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return delegate.fieldsConsumer(state);
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
            return delegate.fieldsProducer(state);
        }
    }

    static final class KnnVectors extends KnnVectorsFormat {
        private final KnnVectorsFormat delegate;

        KnnVectors(KnnVectorsFormat delegate) {
            super(delegate.getName());
            this.delegate = delegate;
        }

        @Override
        public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
            return delegate.fieldsWriter(state);
        }

        @Override
        public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
            return delegate.fieldsReader(state);
        }

        @Override
        public int getMaxDimensions(String fieldName) {
            return delegate.getMaxDimensions(fieldName);
        }
    }
}
