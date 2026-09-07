/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.esql.EsqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsAction;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsResponse;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class EsqlUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;
    private final ProjectResolver projectResolver;

    @Inject
    public EsqlUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        ProjectResolver projectResolver
    ) {
        super(XPackUsageFeatureAction.ESQL.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = client;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {

        EsqlStatsRequest esqlRequest = new EsqlStatsRequest();
        esqlRequest.includeStats(true);
        esqlRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.execute(EsqlStatsAction.INSTANCE, esqlRequest, listener.delegateFailureAndWrap((l, r) -> {
            List<Counters> countersPerNode = r.getNodes()
                .stream()
                .map(EsqlStatsResponse.NodeStatsResponse::getStats)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            Counters mergedCounters = Counters.merge(countersPerNode);
            addInventory(state, mergedCounters);
            EsqlFeatureSetUsage usage = new EsqlFeatureSetUsage(mergedCounters.toNestedMap());
            l.onResponse(new XPackUsageFeatureResponse(usage));
        }));
    }

    private void addInventory(ClusterState state, Counters counters) {
        ProjectMetadata project = projectResolver.getProjectMetadata(state);
        DataSourceMetadata dsMetadata = DataSourceMetadata.get(project);
        DatasetMetadata datasetMetadata = DatasetMetadata.get(project);

        counters.inc("datasources.config.datasources.count", dsMetadata.dataSources().size());
        for (DataSource ds : dsMetadata.dataSources().values()) {
            counters.inc("datasources.config.datasources.by_type." + canonicalType(ds.type()), 1);
        }

        counters.inc("datasources.config.datasets.count", datasetMetadata.datasets().size());
        datasetMetadata.datasets().values().forEach(dataset -> {
            DataSource parent = dsMetadata.get(dataset.dataSource().getName());
            String type = parent != null ? canonicalType(parent.type()) : "unknown";
            counters.inc("datasources.config.datasets.by_datasource_type." + type, 1);
        });
    }

    /**
     * Known Elastic-defined datasource type identifiers. Anything else — including types registered by
     * third-party plugins — is bucketed to {@code "unknown"} so the phone-home payload never carries
     * arbitrary customer-controlled strings.
     * <p>
     * Identifiers: {@code s3} (S3DataSourcePlugin), {@code gcs} (GcsDataSourcePlugin),
     * {@code azure} (AzureDataSourcePlugin), {@code http} and {@code local} (HttpDataSourcePlugin).
     */
    private static final Set<String> KNOWN_TYPES = Set.of("s3", "gcs", "azure", "http", "local");

    /**
     * Maps a datasource type string to a canonical token safe for phone-home emission. The type is
     * constrained by the plugin validator registry at datasource-creation time (only plugin-registered
     * identifiers can appear in cluster state), but third-party plugins could register non-Elastic
     * identifiers, so we close the vocabulary here: known Elastic types pass through lower-cased;
     * anything else becomes {@code "unknown"}.
     */
    private static String canonicalType(String type) {
        if (type == null) {
            return "unknown";
        }
        String lower = type.toLowerCase(Locale.ROOT);
        return KNOWN_TYPES.contains(lower) ? lower : "unknown";
    }
}
