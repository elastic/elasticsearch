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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SourceFilter;

import java.io.IOException;
import java.util.List;

public final class TimeSeriesDimensionsFieldBlockLoader implements BlockLoader {
    private final List<String> fieldNames;

    public TimeSeriesDimensionsFieldBlockLoader(List<String> fieldNames) {
        this.fieldNames = fieldNames;
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
        return new TimeSeriesSource(new SourceFilter(fieldNames.toArray(Strings.EMPTY_ARRAY), null));
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return new StoredFieldsSpec(true, false, java.util.Set.of());
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) {
        throw new UnsupportedOperationException();
    }

    private static class TimeSeriesSource extends BlockStoredFieldsReader {
        private final SourceFilter sourceFilter;

        TimeSeriesSource(SourceFilter sourceFilter) {
            this.sourceFilter = sourceFilter;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            org.elasticsearch.search.lookup.Source source = storedFields.source();
            if (source == null) {
                builder.appendNull();
                return;
            }

            if (sourceFilter != null) {
                source = source.filter(sourceFilter);
            }

            BytesReference sourceRef = source.internalSourceRef();
            if (sourceRef == null) {
                builder.appendNull();
            } else {
                ((BytesRefBuilder) builder).appendBytesRef(sourceRef.toBytesRef());
            }
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.TimeSeriesSource";
        }
    }
}
