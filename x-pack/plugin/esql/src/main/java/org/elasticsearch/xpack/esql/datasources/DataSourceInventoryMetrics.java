/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongAsyncGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collection;
import java.util.List;

/**
 * Master-gated APM gauges for the configuration inventory. The supplier returns an empty
 * collection when this node is not master so non-masters emit no series.
 */
public final class DataSourceInventoryMetrics {

    public static final String DATASOURCES_CURRENT = "es.esql.datasources.config.datasources.current";
    public static final String DATASETS_CURRENT = "es.esql.datasources.config.datasets.current";

    private static final Logger logger = LogManager.getLogger(DataSourceInventoryMetrics.class);

    private final ClusterService clusterService;
    private final DataSourceInventoryCounters inventory;
    private final LongAsyncGauge datasourcesGauge;
    private final LongAsyncGauge datasetsGauge;

    public DataSourceInventoryMetrics(MeterRegistry meterRegistry, ClusterService clusterService, DataSourceInventoryCounters inventory) {
        this.clusterService = clusterService;
        this.inventory = inventory;
        this.datasourcesGauge = meterRegistry.registerLongsAsyncGauge(
            DATASOURCES_CURRENT,
            "Currently registered ES|QL data sources, dimensioned by type and auth",
            "unit",
            this::datasourceObservations
        );
        this.datasetsGauge = meterRegistry.registerLongsAsyncGauge(
            DATASETS_CURRENT,
            "Currently registered ES|QL datasets, dimensioned by type, format, schema, partitioning, compression",
            "unit",
            this::datasetObservations
        );
    }

    private Collection<LongWithAttributes> datasourceObservations() {
        try {
            ProjectMetadata project = projectIfMaster();
            return project == null ? List.of() : inventory.datasourceObservations(project);
        } catch (Exception e) {
            logger.trace("telemetry: datasource inventory gauge failed", e);
            return List.of();
        }
    }

    private Collection<LongWithAttributes> datasetObservations() {
        try {
            ProjectMetadata project = projectIfMaster();
            return project == null ? List.of() : inventory.datasetObservations(project);
        } catch (Exception e) {
            logger.trace("telemetry: dataset inventory gauge failed", e);
            return List.of();
        }
    }

    @Nullable
    private ProjectMetadata projectIfMaster() {
        var state = clusterService.state();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            return null;
        }
        return state.metadata().getProject(ProjectId.DEFAULT);
    }

    LongAsyncGauge datasourcesGauge() {
        return datasourcesGauge;
    }

    LongAsyncGauge datasetsGauge() {
        return datasetsGauge;
    }
}
