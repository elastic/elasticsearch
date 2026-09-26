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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Loads {@code _timeseries} metadata into blocks.
 */
public final class TimeSeriesMetadataFieldBlockLoader implements BlockLoader {

    private final Set<String> metadataFields;
    private final BlockLoader emptyMetadata;
    private final boolean packedDimensions;
    private final List<String> sortedFields;

    public TimeSeriesMetadataFieldBlockLoader(MappedFieldType.BlockLoaderContext context, boolean loadMetrics) {
        this.metadataFields = lookupTimeSeriesMetadataFieldNames(context, loadMetrics);
        this.packedDimensions = ((BlockLoaderFunctionConfig.TimeSeriesMetadata) context.blockLoaderFunctionConfig()).packedDimensions();
        this.sortedFields = metadataFields.stream().sorted(Comparator.comparing(BytesRef::new)).toList();
        // An empty source-path filter means "load all source", not "load no fields". An empty remainder is a real
        // empty object and must never accidentally reintroduce metrics, timestamps, or excluded dimensions.
        this.emptyMetadata = metadataFields.isEmpty() ? BlockLoader.constantBytes(new BytesRef("{}")) : null;
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
        return packedDimensions ? factory.packDimBlockBuilder(expectedCount) : factory.bytesRefs(expectedCount);
    }

    @Override
    public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) throws IOException {
        if (packedDimensions) return null;
        return emptyMetadata == null ? null : emptyMetadata.columnAtATimeReader(context);
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        if (packedDimensions) return new PackedDimensionsReader(breaker, sortedFields);
        return emptyMetadata == null ? new TimeSeriesReader(breaker) : emptyMetadata.rowStrideReader(breaker, context);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        if (emptyMetadata != null) return StoredFieldsSpec.NO_REQUIREMENTS;
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
        return packedDimensions ? "PackedTimeSeriesDimensions" : "TimeSeriesMetadata";
    }

    /**
     * Produces the typed value at the source boundary. Synthetic source may still reconstruct internally, but no
     * keyword metadata block or whole-record JSON adapter is materialized downstream. The local eligible-field list
     * is enforced here even if another reader widens the shared source projection.
     */
    private static final class PackedDimensionsReader extends BlockStoredFieldsReader {
        private static final Object PRESENT_NULL = new Object();
        private final List<String> fields;

        private PackedDimensionsReader(CircuitBreaker breaker, List<String> fields) {
            super(breaker);
            this.fields = fields;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            var names = new ArrayList<BytesRef>(fields.size());
            var values = new ArrayList<BytesRef>(fields.size());
            if (fields.isEmpty() == false) {
                var source = storedFields.source().source();
                for (String field : fields) {
                    Object value = XContentMapValues.extractValue(field, source, PRESENT_NULL);
                    if (value == null) continue;
                    // A caller-selected construction policy; generic packed records preserve these values by default.
                    names.add(new BytesRef(field));
                    try (var encoded = XContentFactory.jsonBuilder()) {
                        encoded.value(normalize(value));
                        values.add(BytesReference.bytes(encoded).toBytesRef());
                    }
                }
            }
            ((PackDimBuilder) builder).append(names.toArray(BytesRef[]::new), values.toArray(BytesRef[]::new));
        }

        private static Object normalize(Object value) {
            if (value == null || value == PRESENT_NULL) return null;
            if (value instanceof List<?> list) return list.stream().map(PackedDimensionsReader::normalize).toList();
            if (value instanceof String
                || value instanceof Boolean
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Double number && Double.isFinite(number)) return value;
            throw new IllegalArgumentException("unsupported packed dimension value [" + value.getClass().getSimpleName() + "]");
        }
    }

    private static final class TimeSeriesReader extends BlockStoredFieldsReader {
        private TimeSeriesReader(CircuitBreaker breaker) {
            super(breaker);
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
            // TODO: support appending BytesReference directly.
            ((BytesRefBuilder) builder).appendBytesRef(toJson(storedFields.source()).toBytesRef());
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.TimeSeries";
        }
    }
}
