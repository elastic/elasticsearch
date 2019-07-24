/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Map.Entry;

import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.LAST_UPDATED_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.PIPELINE_IDS;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.loadPipeline;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.pipelineName;

class LocalResourcesCreator implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(ClusterStateListener.class);

    private final ClusterService clusterService;

    private final AtomicBoolean installingSomething = new AtomicBoolean(false);

    public LocalResourcesCreator(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    public void clusterChanged(ClusterChangedEvent event) {
        // List of templates
        final Map<String, String> templates = Arrays.stream(MonitoringTemplateUtils.TEMPLATE_IDS)
                .collect(Collectors.toMap(MonitoringTemplateUtils::templateName, MonitoringTemplateUtils::loadTemplate));

        setupIfElectedMaster(event.state(), templates, clusterStateChange);
    }

        /**
     * When on the elected master, we setup all resources (mapping types, templates, and pipelines) before we attempt to run the exporter.
     * If those resources do not exist, then we will create them.
     *
     * @param clusterState The current cluster state.
     * @param templates All template names that should exist.
     * @param clusterStateChange {@code true} if a cluster state change caused this call (don't block it!)
     * @return {@code true} indicates that all resources are "ready" and the exporter can be used. {@code false} to stop and wait.
     */
    private boolean setupIfElectedMaster(final ClusterState clusterState, final Map<String, String> templates,
                                         final boolean clusterStateChange) {
        // we are on the elected master
        // Check that there is nothing that could block metadata updates
        if (clusterState.blocks().hasGlobalBlockWithLevel(ClusterBlockLevel.METADATA_WRITE)) {
            logger.debug("waiting until metadata writes are unblocked");
            return false;
        }

        if (installingSomething.get() == true) {
            logger.trace("already installing something, waiting for install to complete");
            return false;
        }

        // build a list of runnables for everything that is missing, but do not start execution
        final List<Runnable> asyncActions = new ArrayList<>();
        final AtomicInteger pendingResponses = new AtomicInteger(0);

        // Check that each required template exists, installing it if needed
        final List<Entry<String, String>> missingTemplates = templates.entrySet()
                .stream()
                .filter((e) -> hasTemplate(clusterState, e.getKey()) == false)
                .collect(Collectors.toList());

        if (missingTemplates.isEmpty() == false) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("template {} not found",
                    missingTemplates.stream().map(Map.Entry::getKey).collect(Collectors.toList())));
            for (Entry<String, String> template : missingTemplates) {
                asyncActions.add(() -> putTemplate(template.getKey(), template.getValue(),
                        new ResponseActionListener<>("template", template.getKey(), pendingResponses)));
            }
        }

        if (useIngest) {
            final List<String> missingPipelines = Arrays.stream(PIPELINE_IDS)
                    .filter(id -> hasIngestPipeline(clusterState, id) == false)
                    .collect(Collectors.toList());

            // if we don't have the ingest pipeline, then install it
            if (missingPipelines.isEmpty() == false) {
                for (final String pipelineId : missingPipelines) {
                    final String pipelineName = pipelineName(pipelineId);
                    logger.debug("pipeline [{}] not found", pipelineName);
                    asyncActions.add(() -> putIngestPipeline(pipelineId,
                                                             new ResponseActionListener<>("pipeline",
                                                                                          pipelineName,
                                                                                          pendingResponses)));
                }
            } else {
                logger.trace("all pipelines found");
            }
        }

        // avoid constantly trying to setup Watcher, which requires a lot of overhead and avoid attempting to setup during a cluster state
        // change
        if (state.get() == State.RUNNING && clusterStateChange == false && canUseWatcher()) {
            final IndexRoutingTable watches = clusterState.routingTable().index(Watch.INDEX);
            final boolean indexExists = watches != null && watches.allPrimaryShardsActive();

            // we cannot do anything with watches until the index is allocated, so we wait until it's ready
            if (watches != null && watches.allPrimaryShardsActive() == false) {
                logger.trace("cannot manage cluster alerts because [.watches] index is not allocated");
            } else if ((watches == null || indexExists) && watcherSetup.compareAndSet(false, true)) {
                getClusterAlertsInstallationAsyncActions(indexExists, asyncActions, pendingResponses);
            }
        }

        if (asyncActions.size() > 0) {
            if (installingSomething.compareAndSet(false, true)) {
                pendingResponses.set(asyncActions.size());
                try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(MONITORING_ORIGIN)) {
                    asyncActions.forEach(Runnable::run);
                }
            } else {
                // let the cluster catch up since requested installations may be ongoing
                return false;
            }
        } else {
            logger.debug("monitoring index templates and pipelines are installed on master node, service can start");
        }

        // everything is setup (or running)
        return true;
    }

    // TODO: duplicated in LocalExporter
    /**
     * Determine if the ingest pipeline for {@code pipelineId} exists in the cluster or not with an appropriate minimum version.
     *
     * @param clusterState The current cluster state
     * @param pipelineId The ID of the pipeline to check (e.g., "3")
     * @return {@code true} if the {@code clusterState} contains the pipeline with an appropriate minimum version
     */
    private boolean hasIngestPipeline(final ClusterState clusterState, final String pipelineId) {
        final String pipelineName = MonitoringTemplateUtils.pipelineName(pipelineId);
        final IngestMetadata ingestMetadata = clusterState.getMetaData().custom(IngestMetadata.TYPE);

        // we ensure that we both have the pipeline and its version represents the current (or later) version
        if (ingestMetadata != null) {
            final PipelineConfiguration pipeline = ingestMetadata.getPipelines().get(pipelineName);

            return pipeline != null && hasValidVersion(pipeline.getConfigAsMap().get("version"), LAST_UPDATED_VERSION);
        }

        return false;
    }

    // TODO: duplicated in LocalExporter
    /**
     * Create the pipeline required to handle past data as well as to future-proof ingestion for <em>current</em> documents (the pipeline
     * is initially empty, but it can be replaced later with one that translates it as-needed).
     * <p>
     * This should only be invoked by the <em>elected</em> master node.
     * <p>
     * Whenever we eventually make a backwards incompatible change, then we need to override any pipeline that already exists that is
     * older than this one. This uses the Elasticsearch version, down to the alpha portion, to determine the version of the last change.
     * <pre><code>
     * {
     *   "description": "...",
     *   "pipelines" : [ ... ],
     *   "version": 6000001
     * }
     * </code></pre>
     */
    private void putIngestPipeline(final String pipelineId, final ActionListener<AcknowledgedResponse> listener) {
        final String pipelineName = pipelineName(pipelineId);
        final BytesReference pipeline = BytesReference.bytes(loadPipeline(pipelineId, XContentType.JSON));
        final PutPipelineRequest request = new PutPipelineRequest(pipelineName, pipeline, XContentType.JSON);

        logger.debug("installing ingest pipeline [{}]", pipelineName);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), MONITORING_ORIGIN, request, listener,
            client.admin().cluster()::putPipeline);
    }

    // TODO: duplicated in LocalExporter
    private boolean hasTemplate(final ClusterState clusterState, final String templateName) {
        final IndexTemplateMetaData template = clusterState.getMetaData().getTemplates().get(templateName);

        return template != null && hasValidVersion(template.getVersion(), LAST_UPDATED_VERSION);
    }

    // TODO: duplicated in LocalExporter
    // FIXME this should use the IndexTemplateMetaDataUpgrader
    private void putTemplate(String template, String source, ActionListener<AcknowledgedResponse> listener) {
        logger.debug("installing template [{}]", template);

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(template).source(source, XContentType.JSON);
        assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), MONITORING_ORIGIN, request, listener,
            client.admin().indices()::putTemplate);
    }

    // TODO: duplicated in LocalExporter
    /**
     * Determine if the {@code version} is defined and greater than or equal to the {@code minimumVersion}.
     *
     * @param version The version to check
     * @param minimumVersion The minimum version required to be a "valid" version
     * @return {@code true} if the version exists and it's &gt;= to the minimum version. {@code false} otherwise.
     */
    private boolean hasValidVersion(final Object version, final long minimumVersion) {
        return version instanceof Number && ((Number)version).intValue() >= minimumVersion;
    }

    // TODO: duplicated in LocalExporter
    /**
     * Install Cluster Alerts (Watches) into the cluster
     *
     * @param asyncActions Asynchronous actions are added to for each Watch.
     * @param pendingResponses Pending response countdown we use to track completion.
     */
    private void getClusterAlertsInstallationAsyncActions(final boolean indexExists, final List<Runnable> asyncActions,
                                                          final AtomicInteger pendingResponses) {
        final boolean canAddWatches = licenseState.isMonitoringClusterAlertsAllowed();

        for (final String watchId : ClusterAlertsUtil.WATCH_IDS) {
            final String uniqueWatchId = ClusterAlertsUtil.createUniqueWatchId(clusterService, watchId);
            final boolean addWatch = canAddWatches && clusterAlertBlacklist.contains(watchId) == false;

            // we aren't sure if no watches exist yet, so add them
            if (indexExists) {
                if (addWatch) {
                    logger.trace("checking monitoring watch [{}]", uniqueWatchId);

                    asyncActions.add(() -> client.execute(GetWatchAction.INSTANCE, new GetWatchRequest(uniqueWatchId),
                        new LocalExporter.GetAndPutWatchResponseActionListener(client, watchId, uniqueWatchId,
                            pendingResponses)));
                } else {
                    logger.trace("pruning monitoring watch [{}]", uniqueWatchId);

                    asyncActions.add(() -> client.execute(DeleteWatchAction.INSTANCE, new DeleteWatchRequest(uniqueWatchId),
                        new LocalExporter.ResponseActionListener<>("watch", uniqueWatchId, pendingResponses)));
                }
            } else if (addWatch) {
                asyncActions.add(() -> putWatch(client, watchId, uniqueWatchId, pendingResponses));
            }
        }
    }

    // TODO: duplicated in LocalExporter
    private void putWatch(final Client client, final String watchId, final String uniqueWatchId,
                          final AtomicInteger pendingResponses) {
        final String watch = ClusterAlertsUtil.loadWatch(clusterService, watchId);

        logger.trace("adding monitoring watch [{}]", uniqueWatchId);

        executeAsyncWithOrigin(client, MONITORING_ORIGIN, PutWatchAction.INSTANCE,
            new PutWatchRequest(uniqueWatchId, new BytesArray(watch), XContentType.JSON),
            new LocalExporter.ResponseActionListener<>("watch", uniqueWatchId, pendingResponses, watcherSetup));
    }

    // TODO: duplicated in LocalExporter
    /**
     * Determine if the cluster can use Watcher.
     *
     * @return {@code true} to use Cluster Alerts.
     */
    private boolean canUseWatcher() {
        return XPackSettings.WATCHER_ENABLED.get(config.settings()) &&
            CLUSTER_ALERTS_MANAGEMENT_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());
    }}
