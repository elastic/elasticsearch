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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Load {@code _timeseries} into blocks.
 */
public final class TimeSeriesMetadataFieldBlockLoader implements BlockLoader {

    private final Set<String> dimensions;

    public TimeSeriesMetadataFieldBlockLoader(MappedFieldType.BlockLoaderContext context) {
        this.dimensions = dimensionFields(context);
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

    private static Set<String> dimensionFields(MappedFieldType.BlockLoaderContext ctx) {
        if (ctx.indexSettings().getMode() == IndexMode.TIME_SERIES) {
            IndexMetadata indexMetadata = ctx.indexSettings().getIndexMetadata();
            List<String> dimensionFieldsFromSettings = indexMetadata.getTimeSeriesDimensions();
            if (dimensionFieldsFromSettings != null && dimensionFieldsFromSettings.isEmpty() == false) {
                return new LinkedHashSet<>(dimensionFieldsFromSettings);
            }

            Set<String> dimensionFields = new LinkedHashSet<>();
            MappingLookup mappingLookup = ctx.mappingLookup();
            for (Mapper mapper : mappingLookup.fieldMappers()) {
                if (mapper instanceof FieldMapper fieldMapper) {
                    MappedFieldType fieldType = fieldMapper.fieldType();
                    if (fieldType.isDimension()) {
                        dimensionFields.add(fieldType.name());
                    }
                }
            }
            return dimensionFields;
        }
        throw new IllegalStateException("The TimeSeriesMetadataFieldBlockLoader cannot be used in non-time series mode.");
    }
}
