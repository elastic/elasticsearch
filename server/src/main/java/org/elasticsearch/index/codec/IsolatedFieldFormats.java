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
 * Thin delegating Lucene format wrappers that isolate one field's data into its own segment files.
 * <p>
 * Lucene's per-field formats key their file layout on the format <em>instance</em> (bumping the file suffix per format
 * name), so handing out a distinct wrapper instance per field gives each field its own suffix, and therefore its own
 * files. The wrapper reports the delegate's registered name, so reads resolve the real format via SPI; it only exists on
 * the write path and is never registered with or looked up through the codec SPI.
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
