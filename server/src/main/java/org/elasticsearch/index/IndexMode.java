/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentDimensions;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
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
            IndexMode.validateTimeSeriesSettings(settings);
        }

        @Override
        public void validateMapping(MappingLookup lookup) {};

        @Override
        public void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting) {}

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            if (isDataStream) {
                MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
            }
        }

        @Override
        public CompressedXContent getDefaultMapping() {
            return null;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper() {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public IdFieldMapper idFieldMapperWithoutFieldData() {
            return ProvidedIdFieldMapper.NO_FIELD_DATA;
        }

        @Override
        public IdFieldMapper buildIdFieldMapper(BooleanSupplier fieldDataEnabled) {
            return new ProvidedIdFieldMapper(fieldDataEnabled);
        }

        @Override
        public DocumentDimensions buildDocumentDimensions(IndexSettings settings) {
            return new DocumentDimensions.OnlySingleValueAllowed();
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {}

        @Override
        public boolean isSyntheticSourceEnabled() {
            return false;
        }
    },
    TIME_SERIES("time_series") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            if (settings.get(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING) != Integer.valueOf(1)) {
                throw new IllegalArgumentException(error(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING));
            }
            for (Setting<?> unsupported : TIME_SERIES_UNSUPPORTED) {
                if (false == Objects.equals(unsupported.getDefault(Settings.EMPTY), settings.get(unsupported))) {
                    throw new IllegalArgumentException(error(unsupported));
                }
            }
            checkSetting(settings, IndexMetadata.INDEX_ROUTING_PATH);
        }

        private static void checkSetting(Map<Setting<?>, Object> settings, Setting<?> setting) {
            if (Objects.equals(setting.getDefault(Settings.EMPTY), settings.get(setting))) {
                throw new IllegalArgumentException(tsdbMode() + " requires a non-empty [" + setting.getKey() + "]");
            }
        }

        private static String error(Setting<?> unsupported) {
            return tsdbMode() + " is incompatible with [" + unsupported.getKey() + "]";
        }

        @Override
        public void validateMapping(MappingLookup lookup) {
            if (lookup.nestedLookup() != NestedLookup.EMPTY) {
                throw new IllegalArgumentException("cannot have nested fields when index is in " + tsdbMode());
            }
            if (((RoutingFieldMapper) lookup.getMapper(RoutingFieldMapper.NAME)).required()) {
                throw new IllegalArgumentException(routingRequiredBad());
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
        public CompressedXContent getDefaultMapping() {
            return DEFAULT_TIME_SERIES_TIMESTAMP_MAPPING;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return new TimestampBounds(indexMetadata.getTimeSeriesStart(), indexMetadata.getTimeSeriesEnd());
        }

        private static String routingRequiredBad() {
            return "routing is forbidden on CRUD operations that target indices in " + tsdbMode();
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper() {
            return TimeSeriesIdFieldMapper.INSTANCE;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            return TimeSeriesRoutingHashFieldMapper.INSTANCE;
        }

        public IdFieldMapper idFieldMapperWithoutFieldData() {
            return TsidExtractingIdFieldMapper.INSTANCE;
        }

        @Override
        public IdFieldMapper buildIdFieldMapper(BooleanSupplier fieldDataEnabled) {
            // We don't support field data on TSDB's _id
            return TsidExtractingIdFieldMapper.INSTANCE;
        }

        @Override
        public DocumentDimensions buildDocumentDimensions(IndexSettings settings) {
            IndexRouting.ExtractFromSource routing = (IndexRouting.ExtractFromSource) settings.getIndexRouting();
            return new TimeSeriesIdFieldMapper.TimeSeriesIdBuilder(routing.builder());
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return true;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {
            if (sourceFieldMapper.isSynthetic() == false) {
                throw new IllegalArgumentException("time series indices only support synthetic source");
            }
        }

        @Override
        public boolean isSyntheticSourceEnabled() {
            return true;
        }
    },
    LOGSDB("logsdb") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            IndexMode.validateTimeSeriesSettings(settings);
        }

        @Override
        public void validateMapping(MappingLookup lookup) {}

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
        public CompressedXContent getDefaultMapping() {
            return DEFAULT_LOGS_TIMESTAMP_MAPPING;
        }

        @Override
        public IdFieldMapper buildIdFieldMapper(BooleanSupplier fieldDataEnabled) {
            return new ProvidedIdFieldMapper(fieldDataEnabled);
        }

        @Override
        public IdFieldMapper idFieldMapperWithoutFieldData() {
            return ProvidedIdFieldMapper.NO_FIELD_DATA;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper() {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public DocumentDimensions buildDocumentDimensions(IndexSettings settings) {
            return new DocumentDimensions.OnlySingleValueAllowed();
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {
            if (sourceFieldMapper.isSynthetic() == false) {
                throw new IllegalArgumentException("Indices with with index mode [" + IndexMode.LOGSDB + "] only support synthetic source");
            }
        }

        @Override
        public boolean isSyntheticSourceEnabled() {
            return true;
        }

        @Override
        public String getDefaultCodec() {
            return CodecService.BEST_COMPRESSION_CODEC;
        }
    };

    private static void validateTimeSeriesSettings(Map<Setting<?>, Object> settings) {
        settingRequiresTimeSeries(settings, IndexMetadata.INDEX_ROUTING_PATH);
        settingRequiresTimeSeries(settings, IndexSettings.TIME_SERIES_START_TIME);
        settingRequiresTimeSeries(settings, IndexSettings.TIME_SERIES_END_TIME);
    }

    private static void settingRequiresTimeSeries(Map<Setting<?>, Object> settings, Setting<?> setting) {
        if (false == Objects.equals(setting.getDefault(Settings.EMPTY), settings.get(setting))) {
            throw new IllegalArgumentException("[" + setting.getKey() + "] requires " + tsdbMode());
        }
    }

    protected static String tsdbMode() {
        return "[" + IndexSettings.MODE.getKey() + "=time_series]";
    }

    public static final CompressedXContent DEFAULT_TIME_SERIES_TIMESTAMP_MAPPING;

    static {
        try {
            DEFAULT_TIME_SERIES_TIMESTAMP_MAPPING = new CompressedXContent(
                ((builder, params) -> builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startObject(DataStreamTimestampFieldMapper.NAME)
                    .field("enabled", true)
                    .endObject()
                    .startObject("properties")
                    .startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH)
                    .field("type", DateFieldMapper.CONTENT_TYPE)
                    .field("ignore_malformed", "false")
                    .endObject()
                    .endObject()
                    .endObject())
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static final CompressedXContent DEFAULT_LOGS_TIMESTAMP_MAPPING;

    static {
        try {
            DEFAULT_LOGS_TIMESTAMP_MAPPING = new CompressedXContent(
                ((builder, params) -> builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startObject(DataStreamTimestampFieldMapper.NAME)
                    .field("enabled", true)
                    .endObject()
                    .startObject("properties")
                    .startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH)
                    .field("type", DateFieldMapper.CONTENT_TYPE)
                    .endObject()
                    .startObject("host.name")
                    .field("type", KeywordFieldMapper.CONTENT_TYPE)
                    .field("ignore_above", 1024)
                    .endObject()
                    .endObject()
                    .endObject())
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
                IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING,
                IndexMetadata.INDEX_ROUTING_PATH,
                IndexSettings.TIME_SERIES_START_TIME,
                IndexSettings.TIME_SERIES_END_TIME
            ),
            TIME_SERIES_UNSUPPORTED.stream()
        ).collect(toSet())
    );

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
    public abstract void validateMapping(MappingLookup lookup);

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
    public abstract CompressedXContent getDefaultMapping();

    /**
     * Build the {@link FieldMapper} for {@code _id}.
     */
    public abstract IdFieldMapper buildIdFieldMapper(BooleanSupplier fieldDataEnabled);

    /**
     * Get the singleton {@link FieldMapper} for {@code _id}. It can never support
     * field data.
     */
    public abstract IdFieldMapper idFieldMapperWithoutFieldData();

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
    public abstract MetadataFieldMapper timeSeriesIdFieldMapper();

    /**
     * Return an instance of the {@link TimeSeriesRoutingHashFieldMapper} that generates
     * the _ts_routing_hash field. The field mapper will be added to the list of the metadata
     * field mappers for the index.
     */
    public abstract MetadataFieldMapper timeSeriesRoutingHashFieldMapper();

    /**
     * How {@code time_series_dimension} fields are handled by indices in this mode.
     */
    public abstract DocumentDimensions buildDocumentDimensions(IndexSettings settings);

    /**
     * @return Whether timestamps should be validated for being withing the time range of an index.
     */
    public abstract boolean shouldValidateTimestamp();

    /**
     * Validates the source field mapper
     */
    public abstract void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper);

    /**
     * @return whether synthetic source is the only allowed source mode.
     */
    public abstract boolean isSyntheticSourceEnabled();

    public String getDefaultCodec() {
        return CodecService.DEFAULT_CODEC;
    }

    /**
     * Parse a string into an {@link IndexMode}.
     */
    public static IndexMode fromString(String value) {
        return switch (value) {
            case "standard" -> IndexMode.STANDARD;
            case "time_series" -> IndexMode.TIME_SERIES;
            case "logsdb" -> IndexMode.LOGSDB;
            default -> throw new IllegalArgumentException(
                "["
                    + value
                    + "] is an invalid index mode, valid modes are: ["
                    + Arrays.stream(IndexMode.values()).map(IndexMode::toString).collect(Collectors.joining())
                    + "]"
            );
        };
    }

    @Override
    public String toString() {
        return getName();
    }
}
