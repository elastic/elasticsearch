/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.datasources.datasource.DataSourceService;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceTelemetryVocabulary.Type;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.DatasetShape;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Cluster-state configuration inventory for phone-home and APM gauges. Must only run after
 * per-node {@code Counters.merge} (see {@code EsqlUsageTransportAction}); never from
 * {@code TransportEsqlStatsAction.nodeOperation} or {@code DataSourceCounters.populate}.
 */
public final class DataSourceInventoryCounters {

    public static final String TYPE_ATTRIBUTE = "es_datasource_type";
    public static final String AUTH_ATTRIBUTE = "es_datasource_auth";
    public static final String FORMAT_ATTRIBUTE = "es_datasource_format";
    public static final String SCHEMA_ATTRIBUTE = "es_datasource_schema";
    public static final String PARTITIONING_ATTRIBUTE = "es_datasource_partitioning";
    public static final String COMPRESSION_ATTRIBUTE = "es_datasource_compression";

    @Nullable
    private final DataSourceService dataSourceService;
    @Nullable
    private final DataSourceModule dataSourceModule;

    public DataSourceInventoryCounters() {
        this(null, null);
    }

    public DataSourceInventoryCounters(@Nullable DataSourceService dataSourceService, @Nullable DataSourceModule dataSourceModule) {
        this.dataSourceService = dataSourceService;
        this.dataSourceModule = dataSourceModule;
    }

    public void populate(ProjectMetadata project, Counters counters) {
        populate(
            project,
            counters,
            dataSourceService == null ? type -> null : dataSourceService::validatorFor,
            dataSourceModule == null ? null : dataSourceModule.codecRegistry()
        );
    }

    /**
     * Writes dense inventory keys then increments from {@code project}. Type/count increments
     * are outside derivation try-blocks so a shape/auth failure cannot change those values.
     */
    public static void populate(
        ProjectMetadata project,
        Counters counters,
        @Nullable Function<String, DataSourceValidator> validatorFor,
        @Nullable DecompressionCodecRegistry codecs
    ) {
        emitDenseZeros(counters);
        DataSourceMetadata dsMetadata = DataSourceMetadata.get(project);
        DatasetMetadata datasetMetadata = DatasetMetadata.get(project);

        counters.inc("datasources.config.datasources.count", dsMetadata.dataSources().size());
        for (DataSource ds : dsMetadata.dataSources().values()) {
            counters.inc("datasources.config.datasources.by_type." + Type.fromTypeId(ds.type()).key(), 1);
            counters.inc("datasources.config.datasources.by_auth." + authOf(ds, validatorFor), 1);
        }

        counters.inc("datasources.config.datasets.count", datasetMetadata.datasets().size());
        for (Dataset dataset : datasetMetadata.datasets().values()) {
            DataSource parent = dsMetadata.get(dataset.dataSource().getName());
            String type = parent != null ? Type.fromTypeId(parent.type()).key() : Type.UNKNOWN.key();
            counters.inc("datasources.config.datasets.by_datasource_type." + type, 1);
            DatasetAttrs attrs = datasetAttrs(dataset, parent, validatorFor, codecs);
            counters.inc("datasources.config.datasets.by_format." + attrs.format, 1);
            counters.inc("datasources.config.datasets.by_schema." + attrs.schema, 1);
            counters.inc("datasources.config.datasets.by_partitioning." + attrs.partitioning, 1);
            counters.inc("datasources.config.datasets.by_compression." + attrs.compression, 1);
        }
    }

    public Collection<LongWithAttributes> datasourceObservations(ProjectMetadata project) {
        return datasourceObservations(project, dataSourceService == null ? type -> null : dataSourceService::validatorFor);
    }

    public Collection<LongWithAttributes> datasetObservations(ProjectMetadata project) {
        return datasetObservations(
            project,
            dataSourceService == null ? type -> null : dataSourceService::validatorFor,
            dataSourceModule == null ? null : dataSourceModule.codecRegistry()
        );
    }

    /**
     * One observation per distinct attribute set, with {@code value} equal to the object count.
     * OTEL last-write-wins on identical attributes, so emitting {@code 1} per object would collapse
     * two anonymous S3 sources to a single series of 1.
     */
    public static Collection<LongWithAttributes> datasourceObservations(
        ProjectMetadata project,
        @Nullable Function<String, DataSourceValidator> validatorFor
    ) {
        DataSourceMetadata dsMetadata = DataSourceMetadata.get(project);
        Map<Map<String, Object>, Long> counts = new LinkedHashMap<>();
        for (DataSource ds : dsMetadata.dataSources().values()) {
            try {
                Map<String, Object> attrs = Map.of(
                    TYPE_ATTRIBUTE,
                    Type.fromTypeId(ds.type()).key(),
                    AUTH_ATTRIBUTE,
                    authOf(ds, validatorFor)
                );
                counts.merge(attrs, 1L, Long::sum);
            } catch (Exception e) {
                // skip this object; the rest of the inventory still publishes
            }
        }
        return observations(counts);
    }

    public static Collection<LongWithAttributes> datasetObservations(
        ProjectMetadata project,
        @Nullable Function<String, DataSourceValidator> validatorFor,
        @Nullable DecompressionCodecRegistry codecs
    ) {
        DataSourceMetadata dsMetadata = DataSourceMetadata.get(project);
        DatasetMetadata datasetMetadata = DatasetMetadata.get(project);
        Map<Map<String, Object>, Long> counts = new LinkedHashMap<>();
        for (Dataset dataset : datasetMetadata.datasets().values()) {
            try {
                DataSource parent = dsMetadata.get(dataset.dataSource().getName());
                String type = parent != null ? Type.fromTypeId(parent.type()).key() : Type.UNKNOWN.key();
                DatasetAttrs attrs = datasetAttrs(dataset, parent, validatorFor, codecs);
                Map<String, Object> dimensions = Map.of(
                    TYPE_ATTRIBUTE,
                    type,
                    FORMAT_ATTRIBUTE,
                    attrs.format,
                    SCHEMA_ATTRIBUTE,
                    attrs.schema,
                    PARTITIONING_ATTRIBUTE,
                    attrs.partitioning,
                    COMPRESSION_ATTRIBUTE,
                    attrs.compression
                );
                counts.merge(dimensions, 1L, Long::sum);
            } catch (Exception e) {
                // skip this object; the rest of the inventory still publishes
            }
        }
        return observations(counts);
    }

    private static Collection<LongWithAttributes> observations(Map<Map<String, Object>, Long> counts) {
        List<LongWithAttributes> out = new ArrayList<>(counts.size());
        for (var entry : counts.entrySet()) {
            out.add(new LongWithAttributes(entry.getValue(), entry.getKey()));
        }
        return out;
    }

    private static void emitDenseZeros(Counters counters) {
        for (Type type : Type.values()) {
            counters.inc("datasources.config.datasources.by_type." + type.key(), 0);
            counters.inc("datasources.config.datasets.by_datasource_type." + type.key(), 0);
        }
        for (String auth : DataSourceInventoryVocabulary.AUTH_MODES) {
            counters.inc("datasources.config.datasources.by_auth." + auth, 0);
        }
        for (String format : DataSourceInventoryVocabulary.FORMATS) {
            counters.inc("datasources.config.datasets.by_format." + format, 0);
        }
        for (String schema : DataSourceInventoryVocabulary.SCHEMAS) {
            counters.inc("datasources.config.datasets.by_schema." + schema, 0);
        }
        for (String partitioning : DataSourceInventoryVocabulary.PARTITIONING) {
            counters.inc("datasources.config.datasets.by_partitioning." + partitioning, 0);
        }
        for (String compression : DataSourceInventoryVocabulary.COMPRESSIONS) {
            counters.inc("datasources.config.datasets.by_compression." + compression, 0);
        }
    }

    private static String authOf(DataSource ds, @Nullable Function<String, DataSourceValidator> validatorFor) {
        try {
            DataSourceValidator validator = validatorFor == null ? null : validatorFor.apply(ds.type());
            if (validator == null) {
                return "unknown";
            }
            return DataSourceInventoryVocabulary.authToken(validator.authModeOrNull(ds.settings().asMap()));
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static DatasetAttrs datasetAttrs(
        Dataset dataset,
        @Nullable DataSource parent,
        @Nullable Function<String, DataSourceValidator> validatorFor,
        @Nullable DecompressionCodecRegistry codecs
    ) {
        String format = "unresolved";
        String compression = "unknown";
        try {
            DatasetShape shape = null;
            if (parent != null && validatorFor != null) {
                DataSourceValidator validator = validatorFor.apply(parent.type());
                if (validator != null) {
                    shape = validator.datasetShape(dataset.settings(), dataset.resource());
                }
            }
            if (shape != null) {
                format = DataSourceInventoryVocabulary.formatToken(shape.format());
                if (shape.compression() != null) {
                    compression = DataSourceInventoryVocabulary.compressionToken(shape.compression());
                } else {
                    compression = "unknown";
                }
            } else if (codecs != null) {
                compression = compressionFallback(dataset.resource(), codecs);
            }
        } catch (Exception e) {
            // keep defaults; type/count already recorded
        }
        return new DatasetAttrs(format, schemaOf(dataset), partitioningOf(dataset), compression);
    }

    private static String compressionFallback(String resource, DecompressionCodecRegistry codecs) {
        if (resource == null) {
            return "unknown";
        }
        int lastDot = resource.lastIndexOf('.');
        if (lastDot < 0 || lastDot == resource.length() - 1) {
            return "uncompressed";
        }
        String ext = resource.substring(lastDot);
        var codec = codecs.byExtension(ext);
        if (codec != null) {
            return DataSourceInventoryVocabulary.compressionToken(codec.name());
        }
        String normalized = ext.startsWith(".") ? ext.toLowerCase(Locale.ROOT) : ("." + ext.toLowerCase(Locale.ROOT));
        if (DataSourceInventoryVocabulary.COMPRESSION_BY_EXTENSION.containsKey(normalized)) {
            return DataSourceInventoryVocabulary.COMPRESSION_BY_EXTENSION.get(normalized);
        }
        return "uncompressed";
    }

    private static String schemaOf(Dataset dataset) {
        try {
            DatasetMapping mapping = dataset.mapping();
            if (mapping == null || mapping.mappings() == null) {
                return "inferred";
            }
            return mapping.mappings().dynamic() == DatasetMapping.Dynamic.FALSE ? "declared_strict" : "declared_dynamic";
        } catch (Exception e) {
            return "inferred";
        }
    }

    private static String partitioningOf(Dataset dataset) {
        try {
            return DataSourceInventoryVocabulary.partitioningToken(
                PartitionConfig.fromConfig(dataset.settings()).strategy().name().toLowerCase(Locale.ROOT)
            );
        } catch (Exception e) {
            return "auto";
        }
    }

    private record DatasetAttrs(String format, String schema, String partitioning, String compression) {}
}
