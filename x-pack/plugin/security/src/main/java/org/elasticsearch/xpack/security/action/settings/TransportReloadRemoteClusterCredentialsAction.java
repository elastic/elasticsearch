/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.action.admin.cluster.RestReloadSecureSettingsAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.LinkedProjectConfig;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteClusterSettings;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.security.Security;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * This is a local-only action which updates remote cluster credentials for remote cluster connections, from keystore settings reloaded via
 * a call to {@link RestReloadSecureSettingsAction}.
 * <p>
 * It's invoked as part of the {@link Security#reload(Settings)} call.
 * <p>
 * This action is largely an implementation detail to work around the fact that Security is a plugin without direct access to many core
 * classes, including the {@link RemoteClusterService} which is required for a credentials reload. A transport action gives us access to
 * the {@link RemoteClusterService} which is injectable but not part of the plugin contract.
 */
public class TransportReloadRemoteClusterCredentialsAction extends TransportAction<
    TransportReloadRemoteClusterCredentialsAction.Request,
    ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportReloadRemoteClusterCredentialsAction.class);

    private final RemoteClusterService remoteClusterService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportReloadRemoteClusterCredentialsAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            ActionTypes.RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION.name(),
            actionFilters,
            transportService.getTaskManager(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        assert Transports.assertNotTransportThread("Remote connection re-building is too much for a transport thread");
        final ClusterState clusterState = clusterService.state();
        final ClusterBlockException clusterBlockException = checkBlock(clusterState);
        if (clusterBlockException != null) {
            throw clusterBlockException;
        }
        // Use a supplier to ensure we resolve cluster settings inside a synchronized block, to prevent race conditions
        final Supplier<Settings> settingsSupplier = () -> {
            final Settings persistentSettings = clusterState.metadata().persistentSettings();
            final Settings transientSettings = clusterState.metadata().transientSettings();
            return Settings.builder().put(request.getSettings(), true).put(persistentSettings, false).put(transientSettings, false).build();
        };
        // Synchronized on the RCS instance to match previous behavior where this functionality was in a synchronized RCS method.
        synchronized (remoteClusterService) {
            updateClusterCredentials(settingsSupplier, listener);
        }
    }

    private void updateClusterCredentials(Supplier<Settings> settingsSupplier, ActionListener<ActionResponse.Empty> listener) {
        final var projectId = projectResolver.getProjectId();
        final var credentialsManager = remoteClusterService.getRemoteClusterCredentialsManager();
        final var staticSettings = clusterService.getSettings();
        final var newSettings = settingsSupplier.get();
        final var result = credentialsManager.updateClusterCredentials(newSettings);
        // We only need to rebuild connections when a credential was newly added or removed for a cluster alias, not if the credential
        // value was updated. Therefore, only consider added or removed aliases
        final int totalConnectionsToRebuild = result.addedClusterAliases().size() + result.removedClusterAliases().size();
        if (totalConnectionsToRebuild == 0) {
            logger.debug("project [{}] no connection rebuilding required after credentials update", projectId);
            listener.onResponse(ActionResponse.Empty.INSTANCE);
            return;
        }
        logger.info("project [{}] rebuilding [{}] connections after credentials update", projectId, totalConnectionsToRebuild);
        try (var connectionRefs = new RefCountingRunnable(() -> listener.onResponse(ActionResponse.Empty.INSTANCE))) {
            for (var clusterAlias : result.addedClusterAliases()) {
                maybeRebuildConnectionOnCredentialsChange(toConfig(projectId, clusterAlias, staticSettings, newSettings), connectionRefs);
            }
            for (var clusterAlias : result.removedClusterAliases()) {
                maybeRebuildConnectionOnCredentialsChange(toConfig(projectId, clusterAlias, staticSettings, newSettings), connectionRefs);
            }
        }
    }

    private void maybeRebuildConnectionOnCredentialsChange(LinkedProjectConfig config, RefCountingRunnable connectionRefs) {
        final var projectId = config.originProjectId();
        final var clusterAlias = config.linkedProjectAlias();
        if (false == remoteClusterService.getRegisteredRemoteClusterNames(projectId).contains(clusterAlias)) {
            // A credential was added or removed before a remote connection was configured.
            // Without an existing connection, there is nothing to rebuild.
            logger.info(
                "project [{}] no connection rebuild required for remote cluster [{}] after credentials change",
                projectId,
                clusterAlias
            );
            return;
        }

        remoteClusterService.updateRemoteCluster(config, true, ActionListener.releaseAfter(new ActionListener<>() {
            @Override
            public void onResponse(RemoteClusterService.RemoteClusterConnectionStatus status) {
                logger.info(
                    "project [{}] remote cluster connection [{}] updated after credentials change: [{}]",
                    projectId,
                    clusterAlias,
                    status
                );
            }

            @Override
            public void onFailure(Exception e) {
                // We don't want to return an error to the upstream listener here since a connection rebuild failure
                // does *not* imply a failure to reload secure settings; however, that's how it would surface in the reload-settings call.
                // Instead, we log a warning which is also consistent with how we handle remote cluster settings updates (logging instead of
                // returning an error)
                logger.warn(
                    () -> "project ["
                        + projectId
                        + "] failed to update remote cluster connection ["
                        + clusterAlias
                        + "] after credentials change",
                    e
                );
            }
        }, connectionRefs.acquire()));
    }

    @FixForMultiProject(description = "Supply the linked project ID when building the LinkedProjectConfig object.")
    private LinkedProjectConfig toConfig(ProjectId projectId, String clusterAlias, Settings staticSettings, Settings newSettings) {
        final var mergedSettings = Settings.builder().put(staticSettings, false).put(newSettings, false).build();
        return RemoteClusterSettings.toConfig(projectId, ProjectId.DEFAULT, clusterAlias, mergedSettings);
    }

    private ClusterBlockException checkBlock(ClusterState clusterState) {
        return clusterState.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    public static class Request extends LegacyActionRequest {
        private final Settings settings;

        public Request(Settings settings) {
            this.settings = settings;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Settings getSettings() {
            return settings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            localOnly();
        }
    }
}
