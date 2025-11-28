/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * Load {@code _timeseries} into blocks.
 */
public final class TimeSeriesMetadataFieldBlockLoader implements BlockLoader {

    private final Set<String> dimensions;

    public TimeSeriesMetadataFieldBlockLoader(Set<String> dimensions) {
        this.dimensions = Collections.unmodifiableSet(dimensions);
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
        return new TimeSeries();
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.withSourcePaths(IgnoredSourceFieldMapper.IgnoredSourceFormat.COALESCED_SINGLE_IGNORED_SOURCE, dimensions);
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) {
        throw new UnsupportedOperationException();
    }

    private static class TimeSeries extends BlockStoredFieldsReader {
        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            // TODO support appending BytesReference
            ((BytesRefBuilder) builder).appendBytesRef(storedFields.source().internalSourceRef().toBytesRef());
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.TimeSeries";
        }
    }
}
