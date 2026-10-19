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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Loads {@code _timeseries} metadata into blocks.
 */
public final class TimeSeriesMetadataFieldBlockLoader implements BlockLoader {

    private final Set<String> metadataFields;
    private final List<Dimension> dimensions;
    private final Function<CircuitBreaker, MetadataEncoder> encoderFactory;

    /** Encodes indexed dimension values in the caller's metadata representation. */
    public interface MetadataEncoder extends Releasable {
        /** The returned bytes remain valid until the next call or until this encoder is closed. */
        BytesRef encode(Map<String, List<String>> labels) throws IOException;
    }

    private record Dimension(String label, IndexFieldData<?> fieldData, DocValueFormat format) {}

    public TimeSeriesMetadataFieldBlockLoader(MappedFieldType.BlockLoaderContext context, boolean loadMetrics) {
        this(lookupTimeSeriesMetadataFieldNames(context, loadMetrics));
    }

    private TimeSeriesMetadataFieldBlockLoader(Set<String> metadataFields) {
        this.metadataFields = metadataFields;
        this.dimensions = null;
        this.encoderFactory = null;
    }

    private TimeSeriesMetadataFieldBlockLoader(List<Dimension> dimensions, Function<CircuitBreaker, MetadataEncoder> encoderFactory) {
        this.metadataFields = Set.of();
        this.dimensions = List.copyOf(dimensions);
        this.encoderFactory = encoderFactory;
    }

    /**
     * Reads typed metadata from indexed dimensions, using the same values as separately loaded dimension columns.
     * Source values can differ after mapping coercions, copy_to, or multi-field indexing.
     */
    public static TimeSeriesMetadataFieldBlockLoader fromDocValues(
        SearchExecutionContext context,
        Set<String> excludedFields,
        UnaryOperator<String> labelName,
        Function<CircuitBreaker, MetadataEncoder> encoderFactory
    ) {
        var excluded = new HashSet<String>();
        for (String name : excludedFields) {
            MappedFieldType field = context.getFieldType(name);
            excluded.add(
                field instanceof FlattenedFieldMapper.KeyedFlattenedFieldType keyed ? keyed.rootName() + "." + keyed.key()
                    : field == null ? name
                    : field.name()
            );
        }
        var dimensions = new ArrayList<Dimension>();
        var labels = new HashSet<String>();
        for (var mapper : context.getMappingLookup().dimensionFieldMappers().values()) {
            MappedFieldType field = mapper.fieldType();
            if (field instanceof FlattenedFieldMapper.RootFlattenedFieldType flattened) {
                for (String key : flattened.dimensions()) {
                    String name = field.name() + "." + key;
                    String label = labelName.apply(name);
                    validateLabelName(labels, label);
                    if (excluded.contains(field.name()) == false && excluded.contains(name) == false) {
                        addDimension(dimensions, label, context.getFieldType(name), context);
                    }
                }
            } else {
                String label = labelName.apply(field.name());
                validateLabelName(labels, label);
                if (excluded.contains(field.name()) == false) {
                    addDimension(dimensions, label, field, context);
                }
            }
        }
        return new TimeSeriesMetadataFieldBlockLoader(dimensions, encoderFactory);
    }

    private static void validateLabelName(Set<String> labels, String label) {
        if (labels.add(label) == false) {
            throw new IllegalArgumentException("Multiple dimension fields resolve to time-series label [" + label + "]");
        }
    }

    private static void addDimension(List<Dimension> dimensions, String label, MappedFieldType field, SearchExecutionContext context) {
        if (field.hasDocValues() == false) {
            throw new IllegalArgumentException("Time-series dimension [" + field.name() + "] must have doc values");
        }
        dimensions.add(
            new Dimension(label, context.getForField(field, MappedFieldType.FielddataOperation.SEARCH), field.docValueFormat(null, null))
        );
    }

    private static Set<String> lookupTimeSeriesMetadataFieldNames(MappedFieldType.BlockLoaderContext context, boolean loadMetrics) {
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

        assert config != null;

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
        return dimensions == null ? null : breaker -> new DocValuesReader(breaker, context);
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        if (dimensions != null) {
            return new DocValuesReader(breaker, context);
        }
        SourceFilter sourceFilter = null;
        if (metadataFields.isEmpty() == false) {
            sourceFilter = new SourceFilter(metadataFields.toArray(String[]::new), null);
        }
        return new TimeSeriesReader(breaker, sourceFilter);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        if (dimensions != null) {
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
        throw new UnsupportedOperationException(IndexMode.TIME_SERIES + " metadata does not support ordinals");
    }

    @Override
    public String toString() {
        return dimensions == null ? "TimeSeriesMetadata" : "TimeSeriesMetadataFromDocValues";
    }

    private static final class TimeSeriesReader extends BlockStoredFieldsReader {
        private final SourceFilter sourceFilter;

        private TimeSeriesReader(CircuitBreaker breaker, SourceFilter sourceFilter) {
            super(breaker);
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
            // Readers share the union of their requested source fields. Apply this reader's exclusions independently.
            ((BytesRefBuilder) builder).appendBytesReference(
                toJson(sourceFilter != null ? storedFields.source().filter(sourceFilter) : Source.empty(XContentType.JSON))
            );
        }

        @Override
        public String toString() {
            return BlockStoredFieldsReader.class.getSimpleName() + ".TimeSeries";
        }
    }

    private final class DocValuesReader extends BlockDocValuesReader implements RowStrideReader {
        private final FormattedDocValues[] numericValues;
        private final SortableBinaryDocValues[] binaryValues;
        private final MetadataEncoder encoder;
        private final long readerReservation;
        private BytesRef emptyValue;
        private long rowReservation;
        private int docId = -1;
        private boolean closed;

        private DocValuesReader(CircuitBreaker breaker, LeafReaderContext context) {
            super(breaker);
            readerReservation = 256L + 64L * dimensions.size();
            breaker.addEstimateBytesAndMaybeBreak(readerReservation, "time-series metadata readers");
            try {
                numericValues = new FormattedDocValues[dimensions.size()];
                binaryValues = new SortableBinaryDocValues[dimensions.size()];
                for (int i = 0; i < dimensions.size(); i++) {
                    Dimension dimension = dimensions.get(i);
                    var leaf = dimension.fieldData().load(context);
                    if (leaf instanceof LeafNumericFieldData) {
                        // In particular, unsigned longs must not pass through the fielddata's double-based byte view.
                        numericValues[i] = leaf.getFormattedValues(dimension.format());
                    } else {
                        binaryValues[i] = leaf.getBytesValues();
                    }
                }
                this.encoder = encoderFactory.apply(breaker);
            } catch (RuntimeException | Error e) {
                breaker.addWithoutBreaking(-readerReservation);
                throw e;
            }
        }

        @Override
        protected int docId() {
            return docId;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (dimensions.isEmpty()) {
                return factory.constantBytes(emptyValue(), docs.count() - offset);
            }
            try (var builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), null, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            assert canReuse(docId);
            if (dimensions.isEmpty()) {
                ((BytesRefBuilder) builder).appendBytesRef(emptyValue());
                return;
            }
            this.docId = docId;
            try {
                reserve(256L + 256L * dimensions.size());
                var labels = new HashMap<String, List<String>>();
                for (int i = 0; i < dimensions.size(); i++) {
                    Dimension dimension = dimensions.get(i);
                    List<String> labelValues;
                    if (numericValues[i] != null) {
                        FormattedDocValues values = numericValues[i];
                        if (values.advanceExact(docId) == false) {
                            continue;
                        }
                        int count = values.docValueCount();
                        reserve(64L + 8L * count);
                        labelValues = new ArrayList<>(count);
                        for (int v = 0; v < count; v++) {
                            reserve(256L);
                            Object value = values.nextValue();
                            labelValues.add(value instanceof BytesRef bytes ? bytes.utf8ToString() : value.toString());
                        }
                    } else {
                        SortableBinaryDocValues values = binaryValues[i];
                        if (values.advanceExact(docId) == false) {
                            continue;
                        }
                        int count = values.docValueCount();
                        reserve(64L + 8L * count);
                        labelValues = new ArrayList<>(count);
                        for (int v = 0; v < count; v++) {
                            BytesRef bytes = values.nextValue();
                            // Reserve before decoding; retain every value until the dictionary has been written.
                            reserve(128L + 4L * bytes.length);
                            Object value = dimension.format().format(bytes);
                            labelValues.add(value instanceof BytesRef formatted ? formatted.utf8ToString() : value.toString());
                        }
                    }
                    labels.put(dimension.label(), labelValues);
                }
                ((BytesRefBuilder) builder).appendBytesRef(encoder.encode(labels));
            } finally {
                breaker.addWithoutBreaking(-rowReservation);
                rowReservation = 0;
            }
        }

        private void reserve(long bytes) {
            breaker.addEstimateBytesAndMaybeBreak(bytes, "time-series metadata normalization");
            rowReservation += bytes;
        }

        private BytesRef emptyValue() throws IOException {
            if (emptyValue == null) {
                emptyValue = encoder.encode(Map.of());
            }
            return emptyValue;
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                try {
                    encoder.close();
                } finally {
                    breaker.addWithoutBreaking(-readerReservation);
                }
            }
        }

        @Override
        public String toString() {
            return TimeSeriesMetadataFieldBlockLoader.this.toString();
        }
    }
}
