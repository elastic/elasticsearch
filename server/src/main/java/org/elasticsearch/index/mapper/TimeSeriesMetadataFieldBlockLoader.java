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
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Load {@code _timeseries} into blocks.
 */
public final class TimeSeriesMetadataFieldBlockLoader implements BlockLoader {

    private final Set<String> metadataFields;

    public TimeSeriesMetadataFieldBlockLoader(MappedFieldType.BlockLoaderContext context, boolean loadMetrics) {
        this.metadataFields = timeSeriesMetadata(context, loadMetrics);
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        return new TimeSeries(breaker);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.withSourcePaths(
            IgnoredSourceFieldMapper.IgnoredSourceFormat.COALESCED_SINGLE_IGNORED_SOURCE,
            metadataFields
        );
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
        protected TimeSeries(CircuitBreaker breaker) {
            super(breaker);
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            // TODO support appending BytesReference
            ((BytesRefBuilder) builder).appendBytesRef(toJson(storedFields.source()).toBytesRef());
        }

        /**
         * The {@code _timeseries} keyword column is documented to contain a JSON-encoded object with the dimension
         * key/value pairs identifying each group. {@link Source#internalSourceRef()} returns bytes in whatever XContent
         * type the underlying {@code _source} happens to use: synthetic source always reconstructs as JSON, but stored
         * source preserves the original encoding (e.g. CBOR for documents written via the Prometheus remote-write
         * endpoint, which builds {@link org.elasticsearch.xcontent.XContentFactory#cborBuilder} requests). Normalize
         * to JSON so the value is a valid keyword regardless of how {@code _source} is stored. When the underlying
         * encoding is already JSON we return the bytes unchanged to avoid a parser/builder round-trip.
         */
        private static BytesReference toJson(Source source) throws IOException {
            BytesReference bytes = source.internalSourceRef();
            XContentType type = source.sourceContentType();
            if (type == XContentType.JSON) {
                return bytes;
            }
            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, bytes, type);
                XContentBuilder json = XContentFactory.jsonBuilder()
            ) {
                parser.nextToken();
                json.copyCurrentStructure(parser);
                return BytesReference.bytes(json);
            }
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.TimeSeries";
        }
    }

    private Set<String> timeSeriesMetadata(MappedFieldType.BlockLoaderContext ctx, boolean loadMetrics) {
        if (ctx.indexSettings().getMode() != IndexMode.TIME_SERIES) {
            throw new IllegalStateException("The TimeSeriesMetadataFieldBlockLoader cannot be used in non-time series mode.");
        }

        assert ctx.blockLoaderFunctionConfig() instanceof BlockLoaderFunctionConfig.TimeSeriesMetadata;
        var config = (BlockLoaderFunctionConfig.TimeSeriesMetadata) ctx.blockLoaderFunctionConfig();

        if (loadMetrics == false) {
            IndexMetadata indexMetadata = ctx.indexSettings().getIndexMetadata();
            List<String> dimensionFieldsFromSettings = indexMetadata.getTimeSeriesDimensions();
            if (dimensionFieldsFromSettings != null && dimensionFieldsFromSettings.isEmpty() == false) {
                Set<String> result = new LinkedHashSet<>(dimensionFieldsFromSettings);
                result.removeAll(config.withoutFields());
                return result;
            }
        }

        Set<String> result = new LinkedHashSet<>();
        MappingLookup mappingLookup = ctx.mappingLookup();
        for (Mapper mapper : mappingLookup.fieldMappers()) {
            if (mapper instanceof FieldMapper fieldMapper) {
                MappedFieldType fieldType = fieldMapper.fieldType();
                if (fieldType.isDimension() && config.withoutFields().contains(fieldType.name()) == false) {
                    result.add(fieldType.name());
                }
                if (loadMetrics && fieldType.getMetricType() != null) {
                    result.add(fieldType.name());
                }
            }
        }

        return result;
    }

    @Override
    public String toString() {
        return "TimeSeriesMetadata";
    }
}
