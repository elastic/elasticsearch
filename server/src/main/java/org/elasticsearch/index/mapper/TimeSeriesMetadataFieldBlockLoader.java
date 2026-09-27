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
import org.elasticsearch.common.bytes.BytesArray;
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
import java.util.Set;

/**
 * Loads {@code _timeseries} metadata into blocks.
 */
public final class TimeSeriesMetadataFieldBlockLoader implements BlockLoader {

    private static final BytesReference EMPTY_OBJECT = new BytesArray("{}");

    private final Set<String> metadataFields;

    public TimeSeriesMetadataFieldBlockLoader(MappedFieldType.BlockLoaderContext context, boolean loadMetrics) {
        this.metadataFields = lookupTimeSeriesMetadataFieldNames(context, loadMetrics);
    }

    private static Set<String> lookupTimeSeriesMetadataFieldNames(MappedFieldType.BlockLoaderContext context, boolean loadMetrics) {
        assert context.blockLoaderFunctionConfig() instanceof BlockLoaderFunctionConfig.TimeSeriesMetadata;

        if (context.indexSettings().getMode().isTsdb() == false) {
            throw new IllegalStateException("TimeSeriesMetadataFieldBlockLoader requires index mode: [ " + IndexMode.TIME_SERIES + " ]");
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
        return new TimeSeriesReader(breaker, metadataFields);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        if (metadataFields.isEmpty()) {
            // nothing left to read: the column is the empty object
            return StoredFieldsSpec.NO_REQUIREMENTS;
        }
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
        private final Set<String> metadataFields;
        private final XContentParserConfiguration filter;

        private TimeSeriesReader(CircuitBreaker breaker, Set<String> metadataFields) {
            super(breaker);
            this.metadataFields = metadataFields;
            this.filter = XContentParserConfiguration.EMPTY.withFiltering(null, metadataFields, Set.of(), false);
        }

        /**
         * Returns the source restricted to this loader's fields, as JSON.
         *
         * The {@code _timeseries} keyword column is documented as a JSON-encoded object containing
         * the dimension key/value pairs that identify a time series. Synthetic source already
         * reconstructs as JSON, but stored source preserves the original content type. For example,
         * documents written through the Prometheus remote-write endpoint may be stored as CBOR.
         *
         * The source handed to {@link #read} is loaded once for every field read alongside, over the
         * union of their source paths, so it may carry dimensions this loader excludes (a second
         * {@code _timeseries} column with fewer exclusions read from the same document). The filter
         * makes each column exactly its own view; a loader with no fields left emits {@code {}}.
         */
        private BytesReference toJson(Source source) throws IOException {
            if (metadataFields.isEmpty()) {
                return EMPTY_OBJECT;
            }
            BytesReference bytes = source.internalSourceRef();
            XContentType contentType = source.sourceContentType();
            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(filter, bytes, contentType);
                XContentBuilder json = XContentFactory.jsonBuilder()
            ) {
                if (parser.nextToken() == null) {
                    return EMPTY_OBJECT;
                }
                json.copyCurrentStructure(parser);
                return BytesReference.bytes(json);
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            // TODO: support appending BytesReference directly.
            ((BytesRefBuilder) builder).appendBytesRef(toJson(storedFields.source()).toBytesRef());
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.TimeSeries";
        }
    }
}
