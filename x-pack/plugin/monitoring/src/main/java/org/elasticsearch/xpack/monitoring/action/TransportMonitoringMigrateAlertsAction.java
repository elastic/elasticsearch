/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsRequest;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsResponse;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsResponse.ExporterMigrationResult;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporter.ExporterResourceStatus;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringMigrationCoordinator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportMonitoringMigrateAlertsAction extends TransportMasterNodeAction<
    MonitoringMigrateAlertsRequest,
    MonitoringMigrateAlertsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportMonitoringMigrateAlertsAction.class);

    private final Client client;
    private final MonitoringMigrationCoordinator migrationCoordinator;
    private final Exporters exporters;

    @Inject
    public TransportMonitoringMigrateAlertsAction(
        Client client,
        Exporters exporters,
        MonitoringMigrationCoordinator migrationCoordinator,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            MonitoringMigrateAlertsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            MonitoringMigrateAlertsRequest::new,
            MonitoringMigrateAlertsResponse::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.client = client;
        this.migrationCoordinator = migrationCoordinator;
        this.exporters = exporters;
    }

    @Override
    protected void masterOperation(
        Task task,
        MonitoringMigrateAlertsRequest request,
        ClusterState state,
        ActionListener<MonitoringMigrateAlertsResponse> listener
    ) throws Exception {
        // First, set the migration coordinator as currently running
        if (migrationCoordinator.tryBlockInstallationTasks() == false) {
            throw new EsRejectedExecutionException("Could not migrate cluster alerts. Migration already in progress.");
        }
        try {
            // Wrap the listener to unblock resource installation before completing
            listener = ActionListener.runBefore(listener, migrationCoordinator::unblockInstallationTasks);
            Settings.Builder decommissionAlertSetting = Settings.builder().put(Monitoring.MIGRATION_DECOMMISSION_ALERTS.getKey(), true);
            client.admin()
                .cluster()
                .prepareUpdateSettings(
                    request.masterNodeTimeout(),
                    /* TODO expose separate ack timeout? use masterNodeTimeout() for now */
                    request.masterNodeTimeout()
                )
                .setPersistentSettings(decommissionAlertSetting)
                .execute(completeOnManagementThread(listener));
        } catch (Exception e) {
            // unblock resource installation if something fails here
            migrationCoordinator.unblockInstallationTasks();
            throw e;
        }
    }

    private ActionListener<ClusterUpdateSettingsResponse> completeOnManagementThread(
        ActionListener<MonitoringMigrateAlertsResponse> delegate
    ) {
        // Send failures to the final listener directly, and on success, fork to management thread and execute best effort alert removal
        return delegate.delegateFailure(
            (l, response) -> threadPool.executor(ThreadPool.Names.MANAGEMENT)
                .execute(ActionRunnable.wrap(l, (listener) -> afterSettingUpdate(listener, response)))
        );
    }

    /**
     * Executed after the settings update has been accepted, this collects the enabled and disabled exporters, requesting each of them
     * to explicitly remove their installed alerts if possible. This makes sure that alerts are removed in a timely fashion instead of
     * waiting for metrics to be bulked into the monitoring cluster.
     */
    private void afterSettingUpdate(
        ActionListener<MonitoringMigrateAlertsResponse> listener,
        ClusterUpdateSettingsResponse clusterUpdateSettingsResponse
    ) {
        logger.info("THREAD NAME: {}" + Thread.currentThread().getName());

        // Ensure positive result
        if (clusterUpdateSettingsResponse.isAcknowledged() == false) {
            listener.onFailure(new ElasticsearchException("Failed to update monitoring migration settings"));
        }

        // iterate over all the exporters and refresh the alerts
        Collection<Exporter> enabledExporters = exporters.getEnabledExporters();
        Collection<Exporter.Config> disabledExporterConfigs = exporters.getDisabledExporterConfigs();

        List<Runnable> refreshTasks = new ArrayList<>();
        AtomicInteger remaining = new AtomicInteger(enabledExporters.size() + disabledExporterConfigs.size());
        List<ExporterResourceStatus> results = Collections.synchronizedList(new ArrayList<>(remaining.get()));
        logger.debug(
            "Exporters in need of refreshing [{}]; enabled [{}], disabled [{}]",
            remaining.get(),
            enabledExporters.size(),
            disabledExporterConfigs.size()
        );

        for (Exporter enabledExporter : enabledExporters) {
            refreshTasks.add(
                ActionRunnable.wrap(
                    resultCollector(enabledExporter.config(), listener, remaining, results),
                    (resultCollector) -> deleteAlertsFromOpenExporter(enabledExporter, resultCollector)
                )
            );
        }
        for (Exporter.Config disabledExporter : disabledExporterConfigs) {
            refreshTasks.add(
                ActionRunnable.wrap(
                    resultCollector(disabledExporter, listener, remaining, results),
                    (resultCollector) -> deleteAlertsFromDisabledExporter(disabledExporter, resultCollector)
                )
            );
        }
        for (Runnable refreshTask : refreshTasks) {
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(refreshTask);
        }
    }

    /**
     * Create an action listener that will collect results and finish the request once all results are in.
     * @param exporterConfig The exporter being refreshed in this operation (in case of failure)
     * @param listener The listener to call after all refresh operations are complete
     * @param remaining The counter used to determine if any other operations are in flight
     * @param results A thread-safe collection to hold results
     */
    private static ActionListener<ExporterResourceStatus> resultCollector(
        final Exporter.Config exporterConfig,
        final ActionListener<MonitoringMigrateAlertsResponse> listener,
        final AtomicInteger remaining,
        final List<ExporterResourceStatus> results
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(ExporterResourceStatus exporterResourceStatus) {
                addStatus(exporterResourceStatus);
            }

            @Override
            public void onFailure(Exception e) {
                // Need the exporter name and type here for reporting purposes. Maybe we need to make multiple of these listeners
                addStatus(ExporterResourceStatus.notReady(exporterConfig.name(), exporterConfig.type(), e));
            }

            private void addStatus(ExporterResourceStatus exporterResourceStatus) {
                results.add(exporterResourceStatus);
                int tasksRemaining = remaining.decrementAndGet();
                if (tasksRemaining == 0) {
                    finalResult();
                }
            }

            private void finalResult() {
                try {
                    List<ExporterMigrationResult> collectedResults = results.stream()
                        .map(
                            status -> new ExporterMigrationResult(
                                status.getExporterName(),
                                status.getExporterType(),
                                status.isComplete(),
                                compileReason(status)
                            )
                        )
                        .toList();
                    MonitoringMigrateAlertsResponse response = new MonitoringMigrateAlertsResponse(collectedResults);
                    listener.onResponse(response);
                } catch (Exception e) {
                    // Make this self contained, we don't want to bubble up exceptions in a way where this listener's
                    // onFailure method could be called multiple times.
                    listener.onFailure(e);
                }
            }

            private static Exception compileReason(ExporterResourceStatus status) {
                // The reason for unsuccessful setup could be multiple exceptions: one or more watches
                // may fail to be removed for any reason.
                List<Exception> exceptions = status.getExceptions();
                if (exceptions == null || exceptions.size() == 0) {
                    return null;
                } else if (exceptions.size() == 1) {
                    return exceptions.get(0);
                } else {
                    // Set first exception as the cause, and the rest as suppressed under it.
                    Exception head = new ElasticsearchException("multiple errors occurred during migration", exceptions.get(0));
                    List<Exception> tail = exceptions.subList(1, exceptions.size());
                    return tail.stream().reduce(head, ExceptionsHelper::useOrSuppress);
                }
            }
        };
    }

    /**
     * Attempts to migrate a given exporter's alerts
     * @param exporter The exporter to migrate
     * @param listener Notified of success or failure
     */
    private static void deleteAlertsFromOpenExporter(Exporter exporter, ActionListener<ExporterResourceStatus> listener) {
        assert exporter.isOpen();
        try {
            exporter.removeAlerts(status -> {
                logger.debug("exporter [{}]: completed setup with status [{}]", exporter.config().name(), status.isComplete());
                // Exporter completed its setup (teardown) successfully or unsuccessfully.
                listener.onResponse(status);
            });
        } catch (Exception e) {
            logger.debug("exporter [" + exporter.config().name() + "]: exception encountered during refresh", e);
            listener.onFailure(e);
        }
    }

    /**
     * Opens a disabled exporter in order to migrate it (best-effort), then makes sure it is closed at completion.
     */
    private void deleteAlertsFromDisabledExporter(Exporter.Config exporterConf, ActionListener<ExporterResourceStatus> listener) {
        Exporter disabledExporter = exporters.openExporter(exporterConf);
        deleteAlertsFromOpenExporter(disabledExporter, ActionListener.runBefore(listener, disabledExporter::close));
    }

    @Override
    protected ClusterBlockException checkBlock(MonitoringMigrateAlertsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
