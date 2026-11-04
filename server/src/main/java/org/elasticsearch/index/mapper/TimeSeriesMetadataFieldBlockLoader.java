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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Loads {@code _timeseries} metadata into blocks.
 */
public final class TimeSeriesMetadataFieldBlockLoader implements BlockLoader {

    private final Set<String> metadataFields;
    @Nullable
    private final SourceFilter sourceFilter;

    public TimeSeriesMetadataFieldBlockLoader(MappedFieldType.BlockLoaderContext context, boolean loadMetrics) {
        this.metadataFields = lookupTimeSeriesMetadataFieldNames(context, loadMetrics);
        this.sourceFilter = metadataFields.isEmpty() ? null : new SourceFilter(metadataFields.toArray(String[]::new), null);
    }

    private static Set<String> lookupTimeSeriesMetadataFieldNames(MappedFieldType.BlockLoaderContext context, boolean loadMetrics) {
        assert context.blockLoaderFunctionConfig() instanceof BlockLoaderFunctionConfig.TimeSeriesMetadata;

        if (context.indexSettings().getMode().isTsdb() == false) {
            throw new IllegalStateException(
                TimeSeriesMetadataFieldBlockLoader.class.getSimpleName() + " requires index mode: [ " + IndexMode.TIME_SERIES + " ]"
            );
        }

        var config = (BlockLoaderFunctionConfig.TimeSeriesMetadata) context.blockLoaderFunctionConfig();
        MappingLookup mappingLookup = context.mappingLookup();

        var dimensionMappers = mappingLookup.dimensionFieldMappers();
        var result = new LinkedHashSet<String>(dimensionMappers.size());
        for (var m : dimensionMappers.values()) {
            result.add(m.fieldType().name());
        }

        for (var skip : config.skipFieldNames()) {
            // Resolve field name (e.g. `cpu`) to canonical form (e.g. `attributes.cpu`)
            var f = mappingLookup.getFieldType(skip);
            result.remove(f != null ? f.name() : skip);
        }

        if (loadMetrics) {
            // Metrics are disjoint from dimensions by TSDB mapping validation and are never excluded.
            for (var m : mappingLookup.metricFieldMappers().values()) {
                result.add(m.fieldType().name());
            }
        }

        return result;
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
        return new TimeSeriesReader(breaker, metadataFields, sourceFilter);
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
        throw new UnsupportedOperationException("_timeseries metadata does not support ordinals");
    }

    @Override
    public String toString() {
        return "TimeSeriesMetadata";
    }

    private static final class TimeSeriesReader extends BlockStoredFieldsReader {
        /** Already JSON, so {@link #toJson} would pass it through unchanged. */
        private static final BytesReference EMPTY_JSON_OBJECT = Source.empty(XContentType.JSON).internalSourceRef();

        private final Set<String> metadataFields;
        /** {@code null} when {@link #metadataFields} is empty, where the column is an empty object, see {@link #read}. */
        @Nullable
        private final SourceFilter sourceFilter;
        /** The paths of the source last read from, and whether it has to be narrowed, see {@link #read}. */
        @Nullable
        private Set<String> cachedSourcePaths;
        private boolean cachedNarrowSource = true;

        private TimeSeriesReader(CircuitBreaker breaker, Set<String> metadataFields, @Nullable SourceFilter sourceFilter) {
            super(breaker);
            this.metadataFields = metadataFields;
            this.sourceFilter = sourceFilter;
        }

        /**
         * Returns source bytes normalized to JSON.
         *
         * The {@code _timeseries} keyword column is documented as a JSON-encoded object containing
         * the dimension key/value pairs that identify a time series. Synthetic source already
         * reconstructs as JSON, but stored source preserves the original content type. For example,
         * documents written through the Prometheus remote-write endpoint may be stored as CBOR.
         *
         * If the source is already JSON, this method returns the original bytes to avoid an
         * unnecessary parser/builder round trip.
         */
        private static BytesReference toJson(Source source) throws IOException {
            BytesReference bytes = source.internalSourceRef();
            XContentType contentType = source.sourceContentType();

            if (contentType == XContentType.JSON) {
                return bytes;
            }

            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, bytes, contentType);
                XContentBuilder json = XContentFactory.jsonBuilder()
            ) {
                parser.nextToken();
                json.copyCurrentStructure(parser);
                return BytesReference.bytes(json);
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            if (sourceFilter == null) {
                // Every dimension is excluded, so there is nothing left to identify the series by.
                ((BytesRefBuilder) builder).appendBytesReference(EMPTY_JSON_OBJECT);
                return;
            }
            Source source = storedFields.source();
            // We expect that read() is called many times with the same sourcePaths() Set and we assume it is immutable
            // So we cache it to avoid an expensive Set equality check on every read
            // In general we check that we only apply the source filter if the paths don't match, as SourceFilter is expensive
            if (storedFields.sourcePaths() != cachedSourcePaths) {
                cachedSourcePaths = storedFields.sourcePaths();
                cachedNarrowSource = metadataFields.equals(cachedSourcePaths) == false;
            }
            if (cachedNarrowSource) {
                source = sourceFilter.filterBytes(source);
            }
            ((BytesRefBuilder) builder).appendBytesReference(toJson(source));
        }

        @Override
        public String toString() {
            return BlockStoredFieldsReader.class.getSimpleName() + ".TimeSeries";
        }
    }
}
