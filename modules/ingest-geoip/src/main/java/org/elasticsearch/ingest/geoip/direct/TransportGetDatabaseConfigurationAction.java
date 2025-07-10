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
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportGetDatabaseConfigurationAction extends TransportNodesAction<
    GetDatabaseConfigurationAction.Request,
    GetDatabaseConfigurationAction.Response,
    GetDatabaseConfigurationAction.NodeRequest,
    GetDatabaseConfigurationAction.NodeResponse,
    List<DatabaseConfigurationMetadata>> {

    private final DatabaseNodeService databaseNodeService;

    @Inject
    public TransportGetDatabaseConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
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
        this.databaseNodeService = databaseNodeService;
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
        final IngestGeoIpMetadata geoIpMeta = clusterService.state()
            .metadata()
            .getProject()
            .custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);
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

    @Override
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
            combinedResults.addAll(
                deduplicateNodeResponses(responses, results.stream().map(result -> result.database().name()).collect(Collectors.toSet()))
            );
            ActionListener.respondAndRelease(
                l,
                new GetDatabaseConfigurationAction.Response(combinedResults, clusterService.getClusterName(), responses, failures)
            );
        });
    }

    /*
     * This deduplicates the nodeResponses by name, favoring the most recent. This is because each node is reporting the local databases
     * that it has, and we don't want to report duplicates to the user. It also filters out any that already exist in the set of
     * preExistingNames. This is because the non-local databases take precedence, so any local database with the same name as a non-local
     * one will not be used.
     * Non-private for unit testing
     */
    static Collection<DatabaseConfigurationMetadata> deduplicateNodeResponses(
        List<GetDatabaseConfigurationAction.NodeResponse> nodeResponses,
        Set<String> preExistingNames
    ) {
        /*
         * Each node reports the list of databases that are in its config/ingest-geoip directory. For the sake of this API we assume all
         * local databases with the same name are the same database, and deduplicate by name and just return the newest.
         */
        return nodeResponses.stream()
            .flatMap(response -> response.getDatabases().stream())
            .collect(
                Collectors.groupingBy(
                    database -> database.database().name(),
                    Collectors.maxBy(Comparator.comparing(DatabaseConfigurationMetadata::modifiedDate))
                )
            )
            .values()
            .stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(database -> preExistingNames.contains(database.database().name()) == false)
            .toList();
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
                            getDatabaseNameForFileName(configDatabase.name()),
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
