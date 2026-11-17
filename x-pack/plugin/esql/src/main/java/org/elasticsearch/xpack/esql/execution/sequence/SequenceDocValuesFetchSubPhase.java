/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.lucene.read.ComputeBlockLoaderFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.SequenceSourceTypes;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.DefaultShardContext;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Loads sequence fields with ordinary ES|QL's mapped block loaders on the PIT's selected Lucene document. This preserves
 * stored fields, keyword delegates, flattened reconstruction and array offsets without loading analyzed text fielddata.
 * Readers and compute blocks use the node's request breaker and are released with the fetch search context.
 */
public final class SequenceDocValuesFetchSubPhase implements FetchSubPhase {
    private final Supplier<BlockFactory> blockFactory;
    private final Supplier<ClusterSettings> clusterSettings;

    /** Defers access to the node's block factory until plugin components have been initialized. */
    public SequenceDocValuesFetchSubPhase(Supplier<BlockFactory> blockFactory, Supplier<ClusterSettings> clusterSettings) {
        this.blockFactory = blockFactory;
        this.clusterSettings = clusterSettings;
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        var extension = (SequenceDocValuesSearchExt) context.getSearchExt(SequenceDocValuesSearchExt.NAME);
        if (extension == null || extension.fields().isEmpty()) {
            return null;
        }
        var searchContext = context.getSearchExecutionContext();
        var shardContext = new DefaultShardContext(0, () -> {}, searchContext, AliasFilter.EMPTY);
        var sizes = loaderSizes(clusterSettings.get());
        List<FieldLoader> fields = new ArrayList<>(extension.fields().size());
        for (var field : extension.fields()) {
            var formatter = valueFormatter(field.field, searchContext.getFieldType(field.field), field.format);
            fields.add(
                new FieldLoader(
                    field.field,
                    shardContext.blockLoader(
                        field.field,
                        false,
                        extension.fieldExtractPreference(),
                        null,
                        (exceptionClass, message) -> HeaderWarning.addWarning("{}: {}", exceptionClass.getName(), message),
                        sizes.ordinals(),
                        sizes.script()
                    ),
                    formatter
                )
            );
        }
        var processor = new Processor(context, fields, blockFactory.get());
        context.addReleasable(processor);
        return processor;
    }

    /** Captures live node/cluster reservation settings once for each fetch processor. */
    static LoaderSizes loaderSizes(ClusterSettings settings) {
        return new LoaderSizes(
            settings.get(PlannerSettings.BLOCK_LOADER_SIZE_ORDINALS),
            settings.get(PlannerSettings.BLOCK_LOADER_SIZE_SCRIPT)
        );
    }

    /** Reader reservations share the cluster defaults and dynamic updates used by ordinary ES|QL. */
    record LoaderSizes(ByteSizeValue ordinals, ByteSizeValue script) {}

    /** Missing/security-hidden fields stay null; every other native cell is interpreted using its current producer. */
    static @Nullable ValueFormatter valueFormatter(String name, @Nullable MappedFieldType fieldType, @Nullable String format) {
        if (fieldType == null) {
            return null;
        }
        DataType type = EsqlDataTypeRegistry.INSTANCE.fromEs(fieldType.familyTypeName(), fieldType.getMetricType()).widenSmallNumeric();
        if (type == DataType.UNSUPPORTED || SequenceSourceTypes.supports(type) == false) {
            throw new IllegalArgumentException(
                "SEQUENCE field [" + name + "] has unsupported live mapped type [" + fieldType.typeName() + "]"
            );
        }
        DataType requestedPoint = SequenceHitReader.GEO_POINT_FORMAT.equals(format) ? DataType.GEO_POINT
            : SequenceHitReader.CARTESIAN_POINT_FORMAT.equals(format) ? DataType.CARTESIAN_POINT
            : null;
        if (requestedPoint != null || type == DataType.GEO_POINT || type == DataType.CARTESIAN_POINT) {
            if (requestedPoint != type) {
                throw new IllegalArgumentException(
                    "SEQUENCE field ["
                        + name
                        + "] has live mapped type ["
                        + fieldType.typeName()
                        + "] incompatible with point format ["
                        + format
                        + "]"
                );
            }
            return value -> {
                if (value != null && (value instanceof BytesRef) == false) {
                    throw new IllegalArgumentException(
                        "SEQUENCE field [" + name + "] requires native WKB point bytes; encoded point doc values are not supported"
                    );
                }
                return value;
            };
        }
        boolean temporal = type == DataType.DATETIME || type == DataType.DATE_NANOS;
        if (name.equals("@timestamp") && format == null) {
            if (temporal == false) {
                throw new IllegalArgumentException("SEQUENCE source [@timestamp] must remain a mapped date or date_nanos");
            }
            return null; // Hidden candidate time checks cardinality without exporting an ordinary mapped value.
        }
        if (SequenceHitReader.INTEGRAL_FORMAT.equals(format)) {
            if (temporal) {
                // A date pattern "0" can be a literal; it does not make the underlying date units an integer value.
                throw new IllegalArgumentException("SEQUENCE integral field [" + name + "] no longer has a numeric type");
            }
            if (type != DataType.UNSIGNED_LONG) {
                fieldType.docValueFormat(format, null); // Preserve the live mapper's ordinary custom-format validation.
            }
            DocValueFormat semantic = fieldType.docValueFormat(null, null);
            return value -> value == null ? null : exactIntegral(name, semanticNumber(name, semantic, value)).toString();
        }
        if (format != null || temporal) {
            return temporalFormatter(name, fieldType, format == null ? SequenceHitReader.TEMPORAL_FORMAT : format);
        }
        if (type == DataType.UNSIGNED_LONG) {
            DocValueFormat semantic = fieldType.docValueFormat(null, null);
            return value -> value == null ? null : exactIntegral(name, semanticNumber(name, semantic, value)).toString();
        }
        if (type == DataType.IP || type == DataType.VERSION) {
            DocValueFormat semantic = fieldType.docValueFormat(null, null);
            return value -> {
                if (value == null) {
                    return null;
                }
                if ((value instanceof BytesRef) == false) {
                    throw new IllegalArgumentException("SEQUENCE field [" + name + "] did not load native mapped bytes");
                }
                return semantic.format((BytesRef) value);
            };
        }
        // The remaining admitted byte producers are UTF8 keyword/text/flattened. Other admitted primitives are native
        // signed numbers, floating values and booleans, whose semantic representation is already unambiguous.
        return null;
    }

    /** Formats the live numeric encoding before checking exactness; unsigned compute longs are biased, unlike signed longs. */
    private static Number semanticNumber(String name, DocValueFormat formatter, Object value) {
        Object semantic;
        if (value instanceof Long || value instanceof Integer) {
            semantic = formatter.format(((Number) value).longValue());
        } else if (value instanceof Double || value instanceof Float) {
            semantic = formatter.format(((Number) value).doubleValue());
        } else {
            throw new IllegalArgumentException("SEQUENCE field [" + name + "] did not load a native numeric value");
        }
        if (semantic instanceof Number number) {
            return number;
        }
        throw new IllegalArgumentException("SEQUENCE field [" + name + "] did not format an actual numeric value");
    }

    private static BigInteger exactIntegral(String name, Number value) {
        if (value instanceof BigInteger number) {
            return number;
        }
        if (value instanceof Long || value instanceof Integer) {
            return BigInteger.valueOf(value.longValue());
        }
        if (value instanceof Double || value instanceof Float) {
            try {
                return new BigDecimal(value.doubleValue()).toBigIntegerExact();
            } catch (ArithmeticException | NumberFormatException e) {
                throw new IllegalArgumentException("SEQUENCE field [" + name + "] does not hold an exact finite integral value", e);
            }
        }
        throw new IllegalArgumentException("SEQUENCE field [" + name + "] did not format an actual numeric value");
    }

    /** Translates a single mapped cell without changing nulls, multivalue shape or the block loader's ownership. */
    @FunctionalInterface
    interface ValueFormatter {
        /** Preserves a live mapped cell's meaning before admission to the prepared consumer schema. */
        Object format(Object value);
    }

    /** Resolves actual date units once per fetch; a missing or security-hidden field keeps ordinary null loading. */
    static @Nullable TemporalFormatter temporalFormatter(String name, @Nullable MappedFieldType fieldType, String format) {
        if (fieldType == null) {
            return null;
        }
        DateFieldMapper.Resolution resolution;
        if (fieldType.typeName().equals(DateFieldMapper.CONTENT_TYPE)) {
            resolution = DateFieldMapper.Resolution.MILLISECONDS;
        } else if (fieldType.typeName().equals(DateFieldMapper.DATE_NANOS_CONTENT_TYPE)) {
            resolution = DateFieldMapper.Resolution.NANOSECONDS;
        } else {
            throw new IllegalArgumentException("SEQUENCE temporal field [" + name + "] no longer has a date or date_nanos type");
        }
        return new TemporalFormatter(name, DateFormatter.forPattern(format), resolution);
    }

    /** Actual cell values use no sort-missing sentinels, including valid long extrema in mapped date fields. */
    record TemporalFormatter(String name, DateFormatter formatter, DateFieldMapper.Resolution resolution) implements ValueFormatter {
        @Override
        public Object format(Object value) {
            if (value == null) {
                return null;
            }
            if ((value instanceof Long) == false) {
                throw new IllegalArgumentException("SEQUENCE temporal field [" + name + "] did not load a native date value");
            }
            return formatter.format(resolution.toInstant((Long) value).atZone(ZoneOffset.UTC));
        }
    }

    private static final class Processor implements FetchSubPhaseProcessor, Releasable {
        private final FetchContext context;
        private final List<FieldLoader> fields;
        private final BlockFactory blockFactory;
        private final ComputeBlockLoaderFactory loaderFactory;
        private final StoredFieldsSpec storedFields;

        private Processor(FetchContext context, List<FieldLoader> fields, BlockFactory blockFactory) {
            this.context = context;
            this.fields = fields;
            this.blockFactory = blockFactory;
            this.loaderFactory = new ComputeBlockLoaderFactory(blockFactory);
            this.storedFields = StoredFieldsSpec.build(fields, field -> field.loader.rowStrideStoredFieldSpec());
        }

        @Override
        public void setNextReader(LeafReaderContext readerContext) {
            for (FieldLoader field : fields) {
                field.setNextReader(readerContext);
            }
        }

        @Override
        public StoredFieldsSpec storedFieldsSpec() {
            return storedFields;
        }

        @Override
        public void process(HitContext hit) throws IOException {
            var stored = new HitStoredFields(hit);
            for (FieldLoader field : fields) {
                try (Block block = field.read(stored, blockFactory, loaderFactory)) {
                    Object value = BlockUtils.toJavaObject(block, 0);
                    List<Object> values;
                    if (value == null) {
                        values = List.of();
                    } else if (value instanceof List<?> list) {
                        values = new ArrayList<>(list.size());
                        for (Object item : list) {
                            values.add(field.format(item));
                        }
                    } else {
                        values = List.of(field.format(value));
                    }
                    var documentField = new DocumentField(field.name, values);
                    // FetchPhase now accounts DocumentField estimates after all sub-phases. Only native backing arrays
                    // remain unaccounted; the existing retained-fetch checker releases their reservation with the response.
                    context.chargeInnerHitsBytes(additionalRetainedBytes(documentField));
                    hit.hit().setDocumentField(documentField);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(Releasables.wrap(fields), loaderFactory);
        }
    }

    /** Charges only copied native arrays beyond the field estimate already owned by FetchPhase. */
    static long additionalRetainedBytes(DocumentField field) {
        return SequenceHitReader.backingBytes(field);
    }

    /** A mutable reader cache kept only for the current leaf and fetch processor's lifetime. */
    static final class FieldLoader implements Releasable {
        private final String name;
        private final BlockLoader loader;
        private final @Nullable ValueFormatter formatter;
        private LeafReaderContext leaf;
        private BlockLoader.ColumnAtATimeReader columnReader;
        private BlockLoader.RowStrideReader rowReader;

        FieldLoader(String name, BlockLoader loader) {
            this(name, loader, null);
        }

        FieldLoader(String name, BlockLoader loader, @Nullable ValueFormatter formatter) {
            this.name = name;
            this.loader = loader;
            this.formatter = formatter;
        }

        private Object format(Object value) {
            return formatter == null ? value : formatter.format(value);
        }

        void setNextReader(LeafReaderContext leaf) {
            close();
            this.leaf = leaf;
        }

        Block read(HitStoredFields stored, BlockFactory blockFactory, ComputeBlockLoaderFactory loaderFactory) throws IOException {
            int doc = stored.hit().docId();
            if (columnReader != null && columnReader.canReuse(doc) == false || rowReader != null && rowReader.canReuse(doc) == false) {
                close();
            }
            if (columnReader == null && rowReader == null) {
                var columnFactory = loader.columnAtATimeReader(leaf);
                if (columnFactory != null) {
                    columnReader = columnFactory.apply(blockFactory.breaker());
                } else {
                    rowReader = loader.rowStrideReader(blockFactory.breaker(), leaf);
                }
            }
            if (columnReader != null) {
                // ComputeBlockLoaderFactory guarantees that mapped loaders produce compute blocks.
                return (Block) columnReader.read(loaderFactory, new SingleDoc(doc), 0, false);
            }
            try (var builder = loader.builder(loaderFactory, 1)) {
                rowReader.read(doc, stored, builder);
                return (Block) builder.build();
            }
        }

        @Override
        public void close() {
            var column = columnReader;
            var row = rowReader;
            columnReader = null;
            rowReader = null;
            Releasables.close(column, row);
        }
    }

    private record SingleDoc(int doc) implements BlockLoader.Docs {
        @Override
        public int count() {
            return 1;
        }

        @Override
        public int get(int index) {
            assert index == 0;
            return doc;
        }

        @Override
        public boolean mayContainDuplicates() {
            return false;
        }
    }

    /** Keeps row-stride readers on one per-event cache, discarded before the next hit. */
    record HitStoredFields(HitContext hit, Map<Object, Object> shared) implements BlockLoader.StoredFields {
        HitStoredFields(HitContext hit) {
            this(hit, new HashMap<>());
        }

        @Override
        public <T> T shared(Object key, IOFunction<BlockLoader.StoredFields, T> compute) throws IOException {
            if (shared.containsKey(key) == false) {
                shared.put(key, compute.apply(this));
            }
            @SuppressWarnings("unchecked") // Each loader uses its key with one value type.
            T value = (T) shared.get(key);
            return value;
        }

        @Override
        public Source source() {
            return hit.source();
        }

        @Override
        public String id() {
            return hit.hit().getId();
        }

        @Override
        public String routing() {
            var routing = hit.loadedFields().get("_routing");
            return routing == null || routing.isEmpty() ? null : routing.getFirst().toString();
        }

        @Override
        public Map<String, List<Object>> storedFields() {
            return hit.loadedFields();
        }

        @Override
        public boolean loaded() {
            return true;
        }
    }
}
