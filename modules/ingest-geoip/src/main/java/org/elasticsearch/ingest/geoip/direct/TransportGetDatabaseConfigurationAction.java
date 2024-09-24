/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.ingest.geoip.DatabaseNodeService;
import org.elasticsearch.ingest.geoip.GeoIpTaskState;
import org.elasticsearch.ingest.geoip.IngestGeoIpMetadata;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.IngestGeoIpFeatures.GET_DATABASE_CONFIGURATION_ACTION_MULTI_NODE;

public class TransportGetDatabaseConfigurationAction extends TransportNodesAction<
    GetDatabaseConfigurationAction.Request,
    GetDatabaseConfigurationAction.Response,
    GetDatabaseConfigurationAction.NodeRequest,
    GetDatabaseConfigurationAction.NodeResponse,
    List<DatabaseConfigurationMetadata>> {

    private final FeatureService featureService;
    private final DatabaseNodeService databaseNodeService;

    @Inject
    public TransportGetDatabaseConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        FeatureService featureService,
        DatabaseNodeService databaseNodeService
    ) {
        super(
            GetDatabaseConfigurationAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            GetDatabaseConfigurationAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.featureService = featureService;
        this.databaseNodeService = databaseNodeService;
    }

    @Override
    protected void doExecute(
        Task task,
        GetDatabaseConfigurationAction.Request request,
        ActionListener<GetDatabaseConfigurationAction.Response> listener
    ) {
        if (featureService.clusterHasFeature(clusterService.state(), GET_DATABASE_CONFIGURATION_ACTION_MULTI_NODE) == false) {
            /*
             * TransportGetDatabaseConfigurationAction used to be a TransportMasterNodeAction, and not all nodes in the cluster have been
             * updated. So we don't want to send node requests to the other nodes because they will blow up. Instead, we just return
             * the information that we used to return from the master node (it doesn't make any difference that this might not be the master
             * node, because we're only reading the clsuter state).
             */
            newResponseAsync(task, request, createActionContext(task, request), List.of(), List.of(), listener);
        } else {
            super.doExecute(task, request, listener);
        }
    }

    protected List<DatabaseConfigurationMetadata> createActionContext(Task task, GetDatabaseConfigurationAction.Request request) {
        final Set<String> ids;
        if (request.getDatabaseIds().length == 0) {
            // if we did not ask for a specific name, then return all databases
            ids = Set.of("*");
        } else {
            ids = new LinkedHashSet<>(Arrays.asList(request.getDatabaseIds()));
        }

        if (ids.size() > 1 && ids.stream().anyMatch(Regex::isSimpleMatchPattern)) {
            throw new IllegalArgumentException(
                "wildcard only supports a single value, please use comma-separated values or a single wildcard value"
            );
        }

        List<DatabaseConfigurationMetadata> results = new ArrayList<>();
        PersistentTasksCustomMetadata tasksMetadata = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(
            clusterService.state()
        );
        for (String id : ids) {
            results.addAll(getWebDatabases(tasksMetadata, id));
            results.addAll(getMaxmindDatabases(clusterService, id));
        }
        return results;
    }

    /*
     * This returns read-only database information about the databases managed by the standard downloader
     */
    private static Collection<DatabaseConfigurationMetadata> getWebDatabases(PersistentTasksCustomMetadata tasksMetadata, String id) {
        List<DatabaseConfigurationMetadata> webDatabases = new ArrayList<>();
        if (tasksMetadata != null) {
            PersistentTasksCustomMetadata.PersistentTask<?> maybeGeoIpTask = tasksMetadata.getTask("geoip-downloader");
            if (maybeGeoIpTask != null) {
                GeoIpTaskState geoIpTaskState = (GeoIpTaskState) maybeGeoIpTask.getState();
                if (geoIpTaskState != null) {
                    Map<String, GeoIpTaskState.Metadata> databases = geoIpTaskState.getDatabases();
                    for (String databaseFileName : databases.keySet()) {
                        String databaseName = getDatabaseNameForFileName(databaseFileName);
                        String databaseId = getDatabaseIdForFileName(DatabaseConfiguration.Web.NAME, databaseFileName);
                        if ((Regex.isSimpleMatchPattern(id) && Regex.simpleMatch(id, databaseId)) || id.equals(databaseId)) {
                            webDatabases.add(
                                new DatabaseConfigurationMetadata(
                                    new DatabaseConfiguration(databaseId, databaseName, new DatabaseConfiguration.Web()),
                                    -1,
                                    databases.get(databaseFileName).lastUpdate()
                                )
                            );
                        }
                    }
                }
            }
        }
        return webDatabases;
    }

    private static String getDatabaseIdForFileName(String providerType, String databaseFileName) {
        return "_" + providerType + "_" + Base64.getEncoder().encodeToString(databaseFileName.getBytes(StandardCharsets.UTF_8));
    }

    private static String getDatabaseNameForFileName(String databaseFileName) {
        return databaseFileName.endsWith(".mmdb")
            ? databaseFileName.substring(0, databaseFileName.length() + 1 - ".mmmdb".length())
            : databaseFileName;
    }

    /*
     * This returns information about databases that are downloaded from maxmind.
     */
    private static Collection<DatabaseConfigurationMetadata> getMaxmindDatabases(ClusterService clusterService, String id) {
        List<DatabaseConfigurationMetadata> maxmindDatabases = new ArrayList<>();
        final IngestGeoIpMetadata geoIpMeta = clusterService.state().metadata().custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);
        if (Regex.isSimpleMatchPattern(id)) {
            for (Map.Entry<String, DatabaseConfigurationMetadata> entry : geoIpMeta.getDatabases().entrySet()) {
                if (Regex.simpleMatch(id, entry.getKey())) {
                    maxmindDatabases.add(entry.getValue());
                }
            }
        } else {
            DatabaseConfigurationMetadata meta = geoIpMeta.getDatabases().get(id);
            if (meta != null) {
                maxmindDatabases.add(meta);
            }
        }
        return maxmindDatabases;
    }

    protected void newResponseAsync(
        Task task,
        GetDatabaseConfigurationAction.Request request,
        List<DatabaseConfigurationMetadata> results,
        List<GetDatabaseConfigurationAction.NodeResponse> responses,
        List<FailedNodeException> failures,
        ActionListener<GetDatabaseConfigurationAction.Response> listener
    ) {
        ActionListener.run(listener, l -> {
            List<DatabaseConfigurationMetadata> combinedResults = new ArrayList<>(results);
            combinedResults.addAll(deduplicateNodeResponses(responses));
            ActionListener.respondAndRelease(
                l,
                new GetDatabaseConfigurationAction.Response(combinedResults, clusterService.getClusterName(), responses, failures)
            );
        });
    }

    private Collection<DatabaseConfigurationMetadata> deduplicateNodeResponses(
        List<GetDatabaseConfigurationAction.NodeResponse> nodeResponses
    ) {
        /*
         * Each node reports the list of databases that are in its config/ingest-geoip directory. For the sake of this API we assume all
         * local databases with the same name are the same database, and deduplicate by name.
         */
        Map<String, DatabaseConfigurationMetadata> resultsFromNodes = new HashMap<>();
        for (GetDatabaseConfigurationAction.NodeResponse nodeResponse : nodeResponses) {
            List<DatabaseConfigurationMetadata> databases = nodeResponse.getDatabases();
            for (DatabaseConfigurationMetadata database : databases) {
                resultsFromNodes.put(database.database().name(), database);
            }
        }
        return resultsFromNodes.values();
    }

    @Override
    protected GetDatabaseConfigurationAction.Response newResponse(
        GetDatabaseConfigurationAction.Request request,
        List<GetDatabaseConfigurationAction.NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        throw new UnsupportedOperationException("Use newResponseAsync instead");
    }

    @Override
    protected GetDatabaseConfigurationAction.NodeRequest newNodeRequest(GetDatabaseConfigurationAction.Request request) {
        return new GetDatabaseConfigurationAction.NodeRequest(request.getDatabaseIds());
    }

    @Override
    protected GetDatabaseConfigurationAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new GetDatabaseConfigurationAction.NodeResponse(in);
    }

    @Override
    protected GetDatabaseConfigurationAction.NodeResponse nodeOperation(GetDatabaseConfigurationAction.NodeRequest request, Task task) {
        final Set<String> ids;
        if (request.getDatabaseIds().length == 0) {
            // if we did not ask for a specific name, then return all databases
            ids = Set.of("*");
        } else {
            ids = new LinkedHashSet<>(Arrays.asList(request.getDatabaseIds()));
        }
        if (ids.size() > 1 && ids.stream().anyMatch(Regex::isSimpleMatchPattern)) {
            throw new IllegalArgumentException(
                "wildcard only supports a single value, please use comma-separated values or a single wildcard value"
            );
        }

        List<DatabaseConfigurationMetadata> results = new ArrayList<>();
        for (String id : ids) {
            results.addAll(getLocalDatabases(databaseNodeService, id));
        }
        return new GetDatabaseConfigurationAction.NodeResponse(transportService.getLocalNode(), results);
    }

    /*
     * This returns information about the databases that users have put in the config/ingest-geoip directory on the node.
     */
    private static List<DatabaseConfigurationMetadata> getLocalDatabases(DatabaseNodeService databaseNodeService, String id) {
        List<DatabaseConfigurationMetadata> localDatabases = new ArrayList<>();
        Map<String, DatabaseNodeService.ConfigDatabaseDetail> configDatabases = databaseNodeService.getConfigDatabasesDetail();
        for (DatabaseNodeService.ConfigDatabaseDetail configDatabase : configDatabases.values()) {
            String databaseId = getDatabaseIdForFileName(DatabaseConfiguration.Local.NAME, configDatabase.name());
            if ((Regex.isSimpleMatchPattern(id) && Regex.simpleMatch(id, databaseId)) || id.equals(databaseId)) {
                localDatabases.add(
                    new DatabaseConfigurationMetadata(
                        new DatabaseConfiguration(
                            databaseId,
                            configDatabase.name(),
                            new DatabaseConfiguration.Local(configDatabase.type())
                        ),
                        -1,
                        configDatabase.buildDateInMillis() == null ? -1 : configDatabase.buildDateInMillis()
                    )
                );
            }
        }
        return localDatabases;
    }
}
