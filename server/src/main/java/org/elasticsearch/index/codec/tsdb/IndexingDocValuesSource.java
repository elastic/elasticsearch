/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;

/**
 * A {@link DocValuesSource} for the add/indexing path that wraps an existing {@link DocValuesProducer},
 * delegating doc values methods to it. Stats are not pre-computed and must be derived by iterating values.
 */
class IndexingDocValuesSource extends DocValuesSource {

    private final DocValuesProducer delegate;

    /**
     * Creates a new indexing doc values source wrapping the given producer.
     *
     * @param delegate the Lucene producer to delegate doc values methods to
     */
    IndexingDocValuesSource(final DocValuesProducer delegate) {
        super(DocValuesConsumerUtil.UNSUPPORTED);
        this.delegate = delegate;
    }

    @Override
    public NumericDocValues getNumeric(final FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(final FieldInfo field) throws IOException {
        return delegate.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(final FieldInfo field) throws IOException {
        return delegate.getSorted(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(final FieldInfo field) throws IOException {
        return delegate.getSortedSet(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(final FieldInfo field) throws IOException {
        return delegate.getSortedNumeric(field);
    }

    // NOTE: close() and checkIntegrity() are inherited as no-ops from EmptyDocValuesProducer.
    // The wrapped producer's lifecycle is managed by Lucene, not by this wrapper.
}
