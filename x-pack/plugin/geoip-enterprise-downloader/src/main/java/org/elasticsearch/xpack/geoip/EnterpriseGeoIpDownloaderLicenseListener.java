/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.ingest.EnterpriseGeoIpTask.EnterpriseGeoIpTaskParams;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.ingest.EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER;

public class EnterpriseGeoIpDownloaderLicenseListener implements LicenseStateListener, ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(EnterpriseGeoIpDownloaderLicenseListener.class);
    // Note: This custom type is IngestGeoIpMetadata.TYPE, but that class is not exposed to this plugin
    static final String INGEST_GEOIP_CUSTOM_METADATA_TYPE = "ingest_geoip";

    private final PersistentTasksService persistentTasksService;
    private final ClusterService clusterService;
    private final XPackLicenseState licenseState;
    private static final LicensedFeature.Momentary ENTERPRISE_GEOIP_FEATURE = LicensedFeature.momentary(
        null,
        XPackField.ENTERPRISE_GEOIP_DOWNLOADER,
        License.OperationMode.PLATINUM
    );
    private final ConcurrentMap<ProjectId, Boolean> licenseIsValid = new ConcurrentHashMap<>();
    private final ConcurrentMap<ProjectId, Boolean> hasIngestGeoIpMetadata = new ConcurrentHashMap<>();
    private final ProjectResolver projectResolver;

    protected EnterpriseGeoIpDownloaderLicenseListener(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ProjectResolver projectResolver
    ) {
        this.persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        this.clusterService = clusterService;
        this.licenseState = licenseState;
        this.projectResolver = projectResolver;
    }

    private volatile boolean licenseStateListenerRegistered;

    public void init() {
        listenForLicenseStateChanges();
        clusterService.addListener(this);
    }

    void listenForLicenseStateChanges() {
        assert licenseStateListenerRegistered == false : "listenForLicenseStateChanges() should only be called once";
        licenseStateListenerRegistered = true;
        licenseState.addListener(this);
    }

    @FixForMultiProject(description = "Replace DEFAULT project after license is project-aware")
    @Override
    public void licenseStateChanged() {
        licenseIsValid.put(ProjectId.DEFAULT, ENTERPRISE_GEOIP_FEATURE.checkWithoutTracking(licenseState));
        maybeUpdateTaskState(clusterService.state().projectState(ProjectId.DEFAULT));
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        event.state().forEachProject(projectState -> {
            ProjectId projectId = projectState.projectId();
            final boolean hasMetadata = event.state().metadata().getProject(projectId).custom(INGEST_GEOIP_CUSTOM_METADATA_TYPE) != null;
            hasIngestGeoIpMetadata.put(projectId, hasMetadata);
            final boolean ingestGeoIpCustomMetaChangedInEvent = event.metadataChanged()
                && event.customMetadataChanged(projectId, INGEST_GEOIP_CUSTOM_METADATA_TYPE);
            final boolean masterNodeChanged = Objects.equals(
                event.state().nodes().getMasterNode(),
                event.previousState().nodes().getMasterNode()
            ) == false;
            /*
             * We don't want to potentially start the task on every cluster state change, so only maybeUpdateTaskState
             * if this cluster change event involved the modification of custom geoip metadata OR a master node change
             */
            if (ingestGeoIpCustomMetaChangedInEvent || (masterNodeChanged && hasIngestGeoIpMetadata.getOrDefault(projectId, false))) {
                maybeUpdateTaskState(projectState);
            }
        });
    }

    private void maybeUpdateTaskState(ProjectState projectState) {
        ProjectId projectId = projectState.projectId();
        // We should only start/stop task from single node, master is the best as it will go through it anyway
        if (projectState.cluster().nodes().isLocalNodeElectedMaster()) {
            if (licenseIsValid.getOrDefault(projectId, false)) {
                if (hasIngestGeoIpMetadata.getOrDefault(projectId, false)) {
                    ensureTaskStarted(projectId);
                }
            } else {
                ensureTaskStopped(projectId);
            }
        }
    }

    private void ensureTaskStarted(ProjectId projectId) {
        assert licenseIsValid.getOrDefault(projectId, false) : "Task should never be started without valid license";
        persistentTasksService.sendProjectStartRequest(
            projectId,
            getTaskId(projectId, projectResolver.supportsMultipleProjects()),
            ENTERPRISE_GEOIP_DOWNLOADER,
            new EnterpriseGeoIpTaskParams(),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.wrap(r -> logger.debug("Started enterprise geoip downloader task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.error("failed to create enterprise geoip downloader task", e);
                }
            })
        );
    }

    private void ensureTaskStopped(ProjectId projectId) {
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener = ActionListener.wrap(
            r -> logger.debug("Stopped enterprise geoip downloader task"),
            e -> {
                Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                if (t instanceof ResourceNotFoundException == false) {
                    logger.error("failed to remove enterprise geoip downloader task", e);
                }
            }
        );
        persistentTasksService.sendRemoveRequest(
            getTaskId(projectId, projectResolver.supportsMultipleProjects()),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            listener
        );
    }

    protected static String getTaskId(ProjectId projectId, boolean supportsMultipleProjects) {
        return supportsMultipleProjects ? projectId + "/" + ENTERPRISE_GEOIP_DOWNLOADER : ENTERPRISE_GEOIP_DOWNLOADER;
    }
}
