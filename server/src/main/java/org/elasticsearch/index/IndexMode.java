/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.RoutingFields;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * "Mode" that controls which behaviors and settings an index supports.
 * <p>
 * For the most part this class concentrates on validating settings and
 * mappings. Most different behavior is controlled by forcing settings
 * to be set or not set and by enabling extra fields in the mapping.
 */
public enum IndexMode {
    STANDARD("standard") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            validateRoutingPathSettings(settings);
        }

        @Override
        public void validateMapping(MappingLookup lookup, Settings settings) {};

        @Override
        public void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting) {}

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            if (isDataStream) {
                MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
            }
        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return null;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public Function<String, String> idTransformerForReindex() {
            return id -> id;
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            return RoutingFields.Noop.INSTANCE;
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {}

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.STORED;
        }
    },
    TIME_SERIES("time_series") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            if (settings.get(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING) != Integer.valueOf(1)) {
                throw new IllegalArgumentException(error(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING));
            }

            var settingsWithIndexMode = Settings.builder().put(IndexSettings.MODE.getKey(), getName()).build();

            for (Setting<?> unsupported : TIME_SERIES_UNSUPPORTED) {
                if (false == Objects.equals(unsupported.getDefault(settingsWithIndexMode), settings.get(unsupported))) {
                    throw new IllegalArgumentException(error(unsupported));
                }
            }
            if (Boolean.TRUE.equals(settings.get(IndexSettings.SLICE_ENABLED))) {
                throw new IllegalArgumentException(
                    "The setting ["
                        + IndexSettings.SLICE_ENABLED.getKey()
                        + "] cannot be used with ["
                        + IndexSettings.MODE.getKey()
                        + "="
                        + IndexMode.TIME_SERIES.getName()
                        + "]."
                );
            }
            Setting<List<String>> routingPath = IndexMetadata.INDEX_ROUTING_PATH;
            if (isEmpty(settings, routingPath) && isEmpty(settings, IndexMetadata.INDEX_DIMENSIONS)) {
                // index.dimensions is a private setting that only gets populated for data streams.
                // We don't include it in the error message to not confuse users that are manually creating time series indices
                // which is the only case where this error can occur.
                throw new IllegalArgumentException(tsdbMode() + " requires a non-empty [" + routingPath.getKey() + "]");
            }
        }

        private static boolean isEmpty(Map<Setting<?>, Object> settings, Setting<List<String>> setting) {
            return Objects.equals(setting.getDefault(Settings.EMPTY), settings.get(setting));
        }

        private static String error(Setting<?> unsupported) {
            return tsdbMode() + " is incompatible with [" + unsupported.getKey() + "]";
        }

        @Override
        public void validateMapping(MappingLookup lookup, Settings settings) {
            if (((RoutingFieldMapper) lookup.getMapper(RoutingFieldMapper.NAME)).required()) {
                throw new IllegalArgumentException(routingRequiredBad());
            }
            validateTemporalityField(lookup, settings);
        }

        private static void validateTemporalityField(MappingLookup lookup, Settings settings) {
            String temporalityFieldName = IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.get(settings);
            if (temporalityFieldName == null || temporalityFieldName.isEmpty()) {
                return;
            }
            MappedFieldType fieldType = lookup.getFieldType(temporalityFieldName);
            if (fieldType == null) {
                // We silently ignore this case because of composable templates:
                // composable templates are verified without being merged with the default mapping
                // This means the temporality field might not exist during verification,
                // but we'll add it automatically when the index is created through the default mapping
                return;
            }
            if (fieldType instanceof KeywordFieldMapper.KeywordFieldType == false) {
                throw new IllegalArgumentException(
                    "["
                        + IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.getKey()
                        + "] field ["
                        + temporalityFieldName
                        + "] must be of type [keyword] but is ["
                        + fieldType.typeName()
                        + "]"
                );
            }
            if (fieldType.isDimension() == false) {
                throw new IllegalArgumentException(
                    "["
                        + IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.getKey()
                        + "] field ["
                        + temporalityFieldName
                        + "] must be a [time_series_dimension]"
                );
            }
        }

        @Override
        public void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting) {
            if (indexRouting != null || searchRouting != null) {
                throw new IllegalArgumentException(routingRequiredBad());
            }
        }

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            // TSDB indices may have a field which stores the metric temporality, the field is defined by the TIME_SERIES_TEMPORALITY_FIELD
            // setting. During mapping validation, the field the setting points to gets included in the default mapping
            // to ensure that it is initialized as expected
            String temporalityField = IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.get(indexSettings.getSettings());
            if (temporalityField != null && temporalityField.isEmpty() == false) {
                return createDefaultMappingWithTemporalityField(temporalityField);
            }
            return DEFAULT_MAPPING_TIMESTAMP;
        }

        private static CompressedXContent createDefaultMappingWithTemporalityField(String temporalityFieldName) {
            try {
                return createDefaultMapping(
                    b -> b.startObject(temporalityFieldName)
                        .field("type", KeywordFieldMapper.CONTENT_TYPE)
                        .field(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM, true)
                        .endObject()
                );
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return new TimestampBounds(indexMetadata.getTimeSeriesStart(), indexMetadata.getTimeSeriesEnd());
        }

        private static String routingRequiredBad() {
            return "routing is forbidden on CRUD operations that target indices in " + tsdbMode();
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            return TimeSeriesIdFieldMapper.getInstance(c);
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            return TimeSeriesRoutingHashFieldMapper.INSTANCE;
        }

        public Function<String, String> idTransformerForReindex() {
            // null the _id so we recalculate it on write
            return id -> null;
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            IndexRouting indexRouting = settings.getIndexRouting();
            if (indexRouting instanceof IndexRouting.ExtractFromSource.ForRoutingPath forRoutingPath) {
                return new RoutingPathFields(forRoutingPath.builder());
            } else if (indexRouting instanceof IndexRouting.ExtractFromSource.ForIndexDimensions) {
                return RoutingFields.Noop.INSTANCE;
            } else {
                throw new IllegalStateException("Index routing strategy not supported for index_mode=time_series: " + indexRouting);
            }
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return true;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {
            if (sourceFieldMapper.enabled() == false) {
                throw new IllegalArgumentException("_source can not be disabled in index using [" + IndexMode.TIME_SERIES + "] index mode");
            }
        }

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.SYNTHETIC;
        }

        @Override
        public boolean isColumnar() {
            return true;
        }
    },
    LOGSDB("logsdb") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            var setting = settings.get(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS);
            if (setting.equals(Boolean.FALSE)) {
                validateRoutingPathSettings(settings);
            }
        }

        @Override
        public void validateMapping(MappingLookup lookup, Settings settings) {}

        @Override
        public void validateAlias(String indexRouting, String searchRouting) {

        }

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            if (isDataStream) {
                MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
            }
        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return indexSettings != null && indexSettings.logsdbAddHostNameField()
                ? DEFAULT_MAPPING_TIMESTAMP_HOSTNAME
                : DEFAULT_MAPPING_TIMESTAMP;
        }

        @Override
        public Function<String, String> idTransformerForReindex() {
            return id -> id;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            return RoutingFields.Noop.INSTANCE;
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {
            if (sourceFieldMapper.enabled() == false) {
                throw new IllegalArgumentException("_source can not be disabled in index using [" + IndexMode.LOGSDB + "] index mode");
            }
        }

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.SYNTHETIC;
        }

        @Override
        public String getDefaultCodec() {
            return CodecService.BEST_COMPRESSION_CODEC;
        }

        @Override
        public boolean isColumnar() {
            return true;
        }
    },
    LOOKUP("lookup") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            final Integer providedNumberOfShards = (Integer) settings.get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING);
            if (providedNumberOfShards != null && providedNumberOfShards != 1) {
                throw new IllegalArgumentException(
                    "index with [lookup] mode must have [index.number_of_shards] set to 1 or unset; provided " + providedNumberOfShards
                );
            }
        }

        @Override
        public void validateMapping(MappingLookup lookup, Settings settings) {};

        @Override
        public void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting) {}

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) {

        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return null;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public Function<String, String> idTransformerForReindex() {
            return id -> id;
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            return RoutingFields.Noop.INSTANCE;
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {}

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.STORED;
        }
    },
    COLUMNAR("columnar") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            validateRoutingPathSettings(settings);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return COLUMNAR_INDEX_MODES_ADDED;
        }

        @Override
        public void validateMapping(MappingLookup lookup, Settings settings) {
            validateNoMappingRuntimeFields(lookup, this);
            validateAllFieldsReconstructableFromDocValues(lookup, this);
        }

        @Override
        public void validateAlias(String indexRouting, String searchRouting) {

        }

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            if (isDataStream) {
                MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
            }
        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return null;
        }

        @Override
        public Function<String, String> idTransformerForReindex() {
            return id -> id;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            return RoutingFields.Noop.INSTANCE;
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {
            if (sourceFieldMapper.enabled() == false) {
                throw new IllegalArgumentException("_source can not be disabled in index using [" + IndexMode.COLUMNAR + "] index mode");
            }
        }

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.SYNTHETIC;
        }

        @Override
        public List<SourceFieldMapper.Mode> supportedSourceModes() {
            return List.of(SourceFieldMapper.Mode.SYNTHETIC, SourceFieldMapper.Mode.COLUMNAR_STORED);
        }

        @Override
        public String getDefaultCodec() {
            return CodecService.BEST_COMPRESSION_CODEC;
        }

        @Override
        public boolean isColumnar() {
            return true;
        }

        @Override
        public boolean isStrictColumnar() {
            return true;
        }
    },
    LOGSDB_COLUMNAR("logsdb_columnar") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            var setting = settings.get(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS);
            if (setting.equals(Boolean.FALSE)) {
                validateRoutingPathSettings(settings);
            }
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return COLUMNAR_INDEX_MODES_ADDED;
        }

        @Override
        public void validateMapping(MappingLookup lookup, Settings settings) {
            validateNoMappingRuntimeFields(lookup, this);
            validateAllFieldsReconstructableFromDocValues(lookup, this);
        }

        @Override
        public void validateAlias(String indexRouting, String searchRouting) {

        }

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            if (isDataStream) {
                MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
            }
        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return indexSettings != null && indexSettings.logsdbAddHostNameField()
                ? DEFAULT_MAPPING_TIMESTAMP_HOSTNAME
                : DEFAULT_MAPPING_TIMESTAMP;
        }

        @Override
        public Function<String, String> idTransformerForReindex() {
            return id -> id;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            return RoutingFields.Noop.INSTANCE;
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {
            if (sourceFieldMapper.enabled() == false) {
                throw new IllegalArgumentException(
                    "_source can not be disabled in index using [" + IndexMode.LOGSDB_COLUMNAR + "] index mode"
                );
            }
        }

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.SYNTHETIC;
        }

        @Override
        public List<SourceFieldMapper.Mode> supportedSourceModes() {
            return List.of(SourceFieldMapper.Mode.SYNTHETIC, SourceFieldMapper.Mode.COLUMNAR_STORED);
        }

        @Override
        public String getDefaultCodec() {
            return CodecService.BEST_COMPRESSION_CODEC;
        }

        @Override
        public boolean isColumnar() {
            return true;
        }

        @Override
        public boolean isStrictColumnar() {
            return true;
        }
    },
    /**
     * Index mode optimized for indexing and searching {@code dense_vector} fields.
     */
    VECTORDB_DOCUMENT("vectordb_document") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {}

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return VECTORDB_DOCUMENT_INDEX_MODE;
        }

        @Override
        public void validateMapping(MappingLookup lookup, Settings settings) {}

        @Override
        public void validateAlias(String indexRouting, String searchRouting) {}

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            if (isDataStream) {
                MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
            }
        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return null;
        }

        @Override
        public Function<String, String> idTransformerForReindex() {
            return id -> id;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            return null;
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            return RoutingFields.Noop.INSTANCE;
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {}

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.STORED;
        }
    };

    static final String HOST_NAME = "host.name";

    private static void validateRoutingPathSettings(Map<Setting<?>, Object> settings) {
        settingRequiresTimeSeries(settings, IndexMetadata.INDEX_ROUTING_PATH);
    }

    private static void settingRequiresTimeSeries(Map<Setting<?>, Object> settings, Setting<?> setting) {
        if (false == Objects.equals(setting.getDefault(Settings.EMPTY), settings.get(setting))) {
            throw new IllegalArgumentException("[" + setting.getKey() + "] requires " + tsdbMode());
        }
    }

    protected static String tsdbMode() {
        return "[" + IndexSettings.MODE.getKey() + "=time_series]";
    }

    /**
     * Rejects mappings that declare runtime fields at the root of the document mapping.
     */
    private static void validateNoMappingRuntimeFields(MappingLookup lookup, IndexMode mode) {
        // TODO Consider including the names of the offending runtime fields in this error message
        // so users can locate the index or component template that introduced them.
        if (lookup.getMapping().getRoot().runtimeFields().isEmpty() == false) {
            throw new IllegalArgumentException("mapping-level runtime fields are not allowed in index using [" + mode + "] index mode");
        }
    }

    /**
     * Columnar index modes rebuild {@code _source} purely from doc-value columns, so every field's {@code _source} must
     * be reconstructable from doc values (synthetic source mode {@code NATIVE}). A field that is not - one with no doc
     * values, or a type whose doc-value encoding cannot rebuild its own source - would otherwise be silently dropped or
     * kept as a lossy source fallback, so reject it instead.
     */
    private static void validateAllFieldsReconstructableFromDocValues(MappingLookup lookup, IndexMode mode) {
        String field = lookup.firstFieldNotReconstructableFromDocValues();
        if (field != null) {
            throw new IllegalArgumentException(
                "field ["
                    + field
                    + "] cannot reconstruct _source from doc values; every field must be reconstructable from doc values in index using ["
                    + mode
                    + "] index mode"
            );
        }
    }

    private static CompressedXContent createDefaultMapping(CheckedConsumer<XContentBuilder, IOException> fieldsCustomizer)
        throws IOException {
        return new CompressedXContent((builder, params) -> {
            builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject(DataStreamTimestampFieldMapper.NAME)
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH)
                .field("type", DateFieldMapper.CONTENT_TYPE)
                .endObject();

            fieldsCustomizer.accept(builder);

            return builder.endObject().endObject();
        });
    }

    private static final CompressedXContent DEFAULT_MAPPING_TIMESTAMP;

    private static final CompressedXContent DEFAULT_MAPPING_TIMESTAMP_HOSTNAME;

    static {
        try {
            DEFAULT_MAPPING_TIMESTAMP = createDefaultMapping(b -> {});
            DEFAULT_MAPPING_TIMESTAMP_HOSTNAME = createDefaultMapping(
                b -> b.startObject(HOST_NAME).field("type", KeywordFieldMapper.CONTENT_TYPE).field("ignore_above", 1024).endObject()
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static final List<Setting<?>> TIME_SERIES_UNSUPPORTED = List.of(
        IndexSortConfig.INDEX_SORT_FIELD_SETTING,
        IndexSortConfig.INDEX_SORT_ORDER_SETTING,
        IndexSortConfig.INDEX_SORT_MODE_SETTING,
        IndexSortConfig.INDEX_SORT_MISSING_SETTING
    );

    static final List<Setting<?>> VALIDATE_WITH_SETTINGS = List.copyOf(
        Stream.concat(
            Stream.of(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING,
                IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING,
                IndexMetadata.INDEX_ROUTING_PATH,
                IndexMetadata.INDEX_DIMENSIONS,
                IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS,
                IndexSettings.SLICE_ENABLED,
                IndexSettings.TIME_SERIES_START_TIME,
                IndexSettings.TIME_SERIES_END_TIME
            ),
            TIME_SERIES_UNSUPPORTED.stream()
        ).collect(toSet())
    );

    public static final FeatureFlag COLUMNAR_FEATURE_FLAG = new FeatureFlag("columnar_index_mode");
    public static final TransportVersion COLUMNAR_INDEX_MODES_ADDED = TransportVersion.fromName("columnar_index_modes_added");

    /**
     * Returns the minimum transport version a recipient node must run to deserialize this index mode.
     * Modes introduced after the initial release override this to return their introduction version.
     */
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.zero();
    }

    /**
     * Returns whether this index mode can be serialized to a node running the given transport version.
     */
    public final boolean supportsVersion(TransportVersion version) {
        return version.supports(getMinimalSupportedVersion());
    }

    /**
     * Returns only the index modes that are available in the current build.
     * Columnar modes are excluded in non-snapshot builds where their feature flag is disabled.
     */
    public static IndexMode[] availableModes() {
        return Arrays.stream(values())
            .filter(m -> COLUMNAR_FEATURE_FLAG.isEnabled() || (m != COLUMNAR && m != LOGSDB_COLUMNAR))
            .toArray(IndexMode[]::new);
    }

    private final String name;

    IndexMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    abstract void validateWithOtherSettings(Map<Setting<?>, Object> settings);

    /**
     * Validate the mapping for this index.
     */
    public abstract void validateMapping(MappingLookup lookup, Settings settings);

    /**
     * Validate aliases targeting this index.
     */
    public abstract void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting);

    /**
     * validate timestamp mapping for this index.
     */
    public abstract void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException;

    /**
     * Get default mapping for this index or {@code null} if there is none.
     */
    @Nullable
    public abstract CompressedXContent getDefaultMapping(IndexSettings indexSettings);

    /**
     * Get the id transformer for reindex to correctly reindex the id into the destination index.
     */
    public abstract Function<String, String> idTransformerForReindex();

    /**
     * @return the time range based on the provided index metadata and index mode implementation.
     * Otherwise <code>null</code> is returned.
     */
    @Nullable
    public abstract TimestampBounds getTimestampBound(IndexMetadata indexMetadata);

    /**
     * Return an instance of the {@link TimeSeriesIdFieldMapper} that generates
     * the _tsid field. The field mapper will be added to the list of the metadata
     * field mappers for the index.
     */
    public abstract MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c);

    /**
     * Return an instance of the {@link TimeSeriesRoutingHashFieldMapper} that generates
     * the _ts_routing_hash field. The field mapper will be added to the list of the metadata
     * field mappers for the index.
     */
    public abstract MetadataFieldMapper timeSeriesRoutingHashFieldMapper();

    /**
     * How {@code time_series_dimension} fields are handled by indices in this mode.
     */
    public abstract RoutingFields buildRoutingFields(IndexSettings settings);

    /**
     * @return Whether timestamps should be validated for being withing the time range of an index.
     */
    public abstract boolean shouldValidateTimestamp();

    /**
     * Validates the source field mapper
     */
    public abstract void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper);

    /**
     * @return default source mode for this mode
     */
    public abstract SourceFieldMapper.Mode defaultSourceMode();

    /**
     * @return source modes supported by this index mode
     */
    public List<SourceFieldMapper.Mode> supportedSourceModes() {
        return List.of(SourceFieldMapper.Mode.DISABLED, SourceFieldMapper.Mode.STORED, SourceFieldMapper.Mode.SYNTHETIC);
    }

    public String getDefaultCodec() {
        return CodecService.DEFAULT_CODEC;
    }

    /**
     * Whether this index mode uses columnar storage optimizations.
     * Columnar modes use specialized codecs for better compression and performance.
     */
    public boolean isColumnar() {
        return false;
    }

    /**
     * Whether this index mode is a strict columnar mode, regardless of index version.
     * In addition to codecs, this includes mappings, e.g., indexing and subobjects configuration.
     */
    public boolean isStrictColumnar() {
        return false;
    }

    /**
     * Parse a string into an {@link IndexMode}.
     */
    public static IndexMode fromString(String value) {
        IndexMode mode = switch (value.toLowerCase(Locale.ROOT)) {
            case "standard" -> IndexMode.STANDARD;
            case "time_series" -> IndexMode.TIME_SERIES;
            case "logsdb" -> IndexMode.LOGSDB;
            case "columnar" -> IndexMode.COLUMNAR;
            case "logsdb_columnar" -> IndexMode.LOGSDB_COLUMNAR;
            case "lookup" -> IndexMode.LOOKUP;
            case "vectordb_document" -> IndexMode.VECTORDB_DOCUMENT;
            default -> throw new IllegalArgumentException(
                "["
                    + value
                    + "] is an invalid index mode, valid modes are: ["
                    + Arrays.stream(IndexMode.values()).map(IndexMode::toString).collect(Collectors.joining(","))
                    + "]"
            );
        };

        if ((mode == IndexMode.COLUMNAR || mode == IndexMode.LOGSDB_COLUMNAR) && COLUMNAR_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException("[" + value + "] index mode is only available in snapshot builds.");
        }
        return mode;
    }

    /**
     * Retrieves the `index.mode` setting and parses it to {@link IndexMode}. When missing, it defaults to standard.
     * Note: This should be used only in cases where it is not relevant to validate the index settings.
     */
    public static IndexMode fromIndexSettingsWithoutValidation(Settings settings) {
        String indexModeLabel = settings.get(IndexSettings.MODE.getKey());
        return indexModeLabel == null ? IndexMode.STANDARD : IndexMode.fromString(indexModeLabel);
    }

    public static final TransportVersion VECTORDB_DOCUMENT_INDEX_MODE = TransportVersion.fromName("vectordb_document_index_mode");

    public static IndexMode readFrom(StreamInput in) throws IOException {
        int mode = in.readByte();
        return switch (mode) {
            case 0 -> STANDARD;
            case 1 -> TIME_SERIES;
            case 2 -> LOGSDB;
            case 3 -> LOOKUP;
            case 4 -> COLUMNAR;
            case 5 -> LOGSDB_COLUMNAR;
            case 6 -> VECTORDB_DOCUMENT;
            default -> throw new IllegalStateException("unexpected index mode [" + mode + "]");
        };
    }

    public static void writeTo(IndexMode indexMode, StreamOutput out) throws IOException {
        if (indexMode.supportsVersion(out.getTransportVersion()) == false) {
            final var message = Strings.format(
                "[%s] doesn't support serialization with transport version [%s]",
                indexMode.getName(),
                out.getTransportVersion()
            );
            throw new IllegalStateException(message);
        }
        final int code = switch (indexMode) {
            case STANDARD -> 0;
            case TIME_SERIES -> 1;
            case LOGSDB -> 2;
            case LOOKUP -> 3;
            case COLUMNAR -> 4;
            case LOGSDB_COLUMNAR -> 5;
            case VECTORDB_DOCUMENT -> 6;
        };
        out.writeByte((byte) code);
    }

    @Override
    public String toString() {
        return getName();
    }

    /**
     * A built-in index setting provider that supplies additional index settings based on the index mode.
     * Currently, only the lookup index mode provides non-empty additional settings.
     */
    public static final class IndexModeSettingsProvider implements IndexSettingProvider {
        @Override
        public void provideAdditionalSettings(
            String indexName,
            String dataStreamName,
            IndexMode templateIndexMode,
            ProjectMetadata projectMetadata,
            Instant resolvedAt,
            Settings indexTemplateAndCreateRequestSettings,
            List<CompressedXContent> combinedTemplateMappings,
            IndexVersion indexVersion,
            Settings.Builder additionalSettings
        ) {
            IndexMode indexMode = templateIndexMode;
            if (indexMode == null) {
                String modeName = indexTemplateAndCreateRequestSettings.get(IndexSettings.MODE.getKey());
                if (modeName != null) {
                    indexMode = IndexMode.valueOf(modeName.toUpperCase(Locale.ROOT));
                }
            }
            if (indexMode == LOOKUP) {
                additionalSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
            }
            if (indexMode == VECTORDB_DOCUMENT) {
                // Force index.mapping.exclude_source_vectors to true
                String excludeSourceVectorsKey = IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey();
                String userValue = indexTemplateAndCreateRequestSettings.get(excludeSourceVectorsKey);
                if (userValue != null && Booleans.parseBoolean(userValue) == false) {
                    throw new IllegalArgumentException(
                        "["
                            + excludeSourceVectorsKey
                            + "] cannot be set to [false] when ["
                            + IndexSettings.MODE.getKey()
                            + "=vectordb_document]"
                    );
                }
                additionalSettings.put(excludeSourceVectorsKey, true);

                // Preload relevant vector index files into the file system cache.
                // Only applied when the user has not explicitly configured [index.store.preload].
                String preloadKey = IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey();
                if (IndexModule.INDEX_STORE_PRE_LOAD_SETTING.exists(indexTemplateAndCreateRequestSettings) == false) {
                    additionalSettings.putList(preloadKey, VECTORDB_DOCUMENT_MODE_PRELOAD_EXTENSIONS);
                }

                // Enable intra-merge parallelism so dense_vector merges can run in parallel within a single merge.
                // Only applied when the user has not set it explicitly.
                String intraMergeKey = IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.getKey();
                if (IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.exists(indexTemplateAndCreateRequestSettings) == false) {
                    additionalSettings.put(intraMergeKey, true);
                }

                // Disable merge IO auto-throttling.
                // Only applied when the user has not set it explicitly.
                String autoThrottleKey = MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey();
                if (MergeSchedulerConfig.AUTO_THROTTLE_SETTING.exists(indexTemplateAndCreateRequestSettings) == false) {
                    additionalSettings.put(autoThrottleKey, false);
                }
            }
        }

        // Vector file extensions preloaded into the file system cache by default for [index.mode=vectordb_document].
        // Excludes:
        // - "vec" (raw vector data) and "clivf" (IVF cluster posting lists): large, streamed from disk on demand
        // - "vem", "vemf", "vemq", "vemb", "vfi", "mivf" (metadata): tiny and already fully read when directory
        // is opened
        static final List<String> VECTORDB_DOCUMENT_MODE_PRELOAD_EXTENSIONS = List.of(
            "vex",    // HNSW graph
            "veq",    // scalar-quantized vector data
            "veb",    // binary-quantized vector data
            "cenivf"  // IVF centroid data
        );
    }
}
