/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TransportMonitoringMigrateAlertsAction extends TransportMasterNodeAction<MonitoringMigrateAlertsRequest,
    MonitoringMigrateAlertsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportMonitoringMigrateAlertsAction.class);

    private static final TimeValue ALERT_MIGRATION_BACK_OFF_RETRY = new TimeValue(5, TimeUnit.SECONDS);
    private static final int ALERT_MIGRATION_MAX_RETRY = 3;

    private final Client client;
    private final Exporters exporters;

    @Inject
    public TransportMonitoringMigrateAlertsAction(Client client, Exporters exporters, TransportService transportService,
                                                  ClusterService clusterService, ThreadPool threadPool,
                                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(MonitoringMigrateAlertsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            MonitoringMigrateAlertsRequest::new, indexNameExpressionResolver);
        this.client = client;
        this.exporters = exporters;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected MonitoringMigrateAlertsResponse read(StreamInput in) throws IOException {
        return new MonitoringMigrateAlertsResponse(in);
    }

    @Override
    protected void masterOperation(Task task, MonitoringMigrateAlertsRequest request, ClusterState state,
                                   ActionListener<MonitoringMigrateAlertsResponse> listener) throws Exception {
        Settings.Builder decommissionAlertSetting = Settings.builder().put(Monitoring.MIGRATION_DECOMMISSION_ALERTS.getKey(), true);
        client.admin().cluster().prepareUpdateSettings().setPersistentSettings(decommissionAlertSetting)
            .execute(afterSettingUpdate(listener));
    }

    /**
     * Executed after the settings update has been accepted, this collects the enabled and disabled exporters, requesting each of them
     * to explicitly remove their installed alerts if possible. This makes sure that alerts are removed in a timely fashion instead of
     * waiting for metrics to be bulked into the monitoring cluster.
     */
    private ActionListener<ClusterUpdateSettingsResponse> afterSettingUpdate(ActionListener<MonitoringMigrateAlertsResponse> listener) {
        return ActionListener.wrap(clusterUpdateSettingsResponse -> {

            // Ensure positive result
            if (!clusterUpdateSettingsResponse.isAcknowledged()) {
                listener.onFailure(new ElasticsearchException("Failed to update monitoring migration settings"));
            }

            // iterate over all the exporters and refresh the alerts
            Collection<Exporter> enabledExporters = exporters.getEnabledExporters();
            Collection<Exporter.Config> disabledExporterConfigs = exporters.getDisabledExporterConfigs();

            List<Runnable> refreshTasks = new ArrayList<>();
            AtomicInteger remaining = new AtomicInteger(enabledExporters.size() + disabledExporterConfigs.size());
            List<ExporterResourceStatus> results = Collections.synchronizedList(new ArrayList<>(remaining.get()));
            logger.debug("Exporters in need of refreshing [{}]; enabled [{}], disabled [{}]", remaining.get(), enabledExporters.size(),
                disabledExporterConfigs.size());

            for (Exporter enabledExporter : enabledExporters) {
                refreshTasks.add(() -> refreshOpenExporter(enabledExporter,
                    resultCollector(enabledExporter.config(), listener, remaining, results),
                    ALERT_MIGRATION_MAX_RETRY, ALERT_MIGRATION_BACK_OFF_RETRY));
            }
            for (Exporter.Config disabledExporter : disabledExporterConfigs) {
                refreshTasks.add(() -> refreshDisabledExporter(disabledExporter,
                    resultCollector(disabledExporter, listener, remaining, results),
                    ALERT_MIGRATION_MAX_RETRY, ALERT_MIGRATION_BACK_OFF_RETRY));
            }
            for (Runnable refreshTask : refreshTasks) {
                threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(refreshTask);
            }
        }, listener::onFailure);
    }

    /**
     * Create an action listener that will collect results and finish the request once all results are in.
     * @param exporterConfig The exporter being refreshed in this operation (in case of failure)
     * @param listener The listener to call after all refresh operations are complete
     * @param remaining The counter used to determine if any other operations are in flight
     * @param results A thread-safe collection to hold results
     */
    private ActionListener<ExporterResourceStatus> resultCollector(final Exporter.Config exporterConfig,
                                                                   final ActionListener<MonitoringMigrateAlertsResponse> listener,
                                                                   final AtomicInteger remaining,
                                                                   final List<ExporterResourceStatus> results) {
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
                    List<ExporterMigrationResult> collectedResults = results.stream().map(status ->
                        new ExporterMigrationResult(
                            status.getExporterName(),
                            status.getExporterType(),
                            status.isReady(),
                            compileReason(status))
                    ).collect(Collectors.toList());
                    MonitoringMigrateAlertsResponse response = new MonitoringMigrateAlertsResponse(collectedResults);
                    listener.onResponse(response);
                } catch (Exception e) {
                    // Make this self contained, we don't want to bubble up exceptions in a way where this listener's
                    // onFailure method could be called multiple times.
                    listener.onFailure(e);
                }
            }

            private Exception compileReason(ExporterResourceStatus status) {
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
     * Attempts to migrate a given exporter's alerts, retrying a number of times in case the exporter is busy doing a previous setup task.
     * @param exporter The exporter to migrate
     * @param listener Notified of success or failure
     * @param retries Number of retries left
     * @param retryBackoff Amount of time to wait between retries
     */
    private void refreshOpenExporter(Exporter exporter, ActionListener<ExporterResourceStatus> listener, int retries,
                                     TimeValue retryBackoff) {
        try {
            exporter.refreshAlerts(status -> {
                if (Exporter.DeployState.IN_PROGRESS == status.getDeployState()) {
                    logger.debug("exporter [{}]: was in progress", exporter.config().name());
                    // Exporter is busy setting up in another thread. Retryable.
                    // We just completed a settings update, so all exporters are fairly new. This retry loop is in case an exporter is
                    // actively accepting results and something else has started its setup process, and also for Local Exporters which hook
                    // into cluster state updates to kick off parts of their setup process.
                    if (retries <= 0) {
                        // Exhausted our attempts at refreshing. This isn't necessarily a failure (Exporter might have just been busy).
                        // It does leave the status of the migration in question. Since the migration just deletes the alerts at the
                        // moment, it's fine to report a not ready status stating it's safe to try again.
                        listener.onResponse(ExporterResourceStatus.notReady(exporter.name(), exporter.config().type(),
                            "Exporter was too busy to acknowledge the migration of cluster alerts. Please attempt migration again."));
                    } else {
                        threadPool.schedule(() -> refreshOpenExporter(exporter, listener, retries - 1, retryBackoff), retryBackoff,
                            ThreadPool.Names.MANAGEMENT);
                    }
                } else {
                    logger.debug("exporter [{}]: completed setup with status [{}]", exporter.config().name(), status.getDeployState());
                    // Exporter completed its setup (teardown) successfully or unsuccessfully.
                    listener.onResponse(status);
                }
            });
        } catch (Exception e) {
            logger.debug("exporter [" + exporter.config().name() + "]: exception encountered during refresh", e);
            listener.onFailure(e);
        }
    }

    /**
     * Opens a disabled exporter in order to migrate it (best-effort), then makes sure it is closed at completion.
     */
    private void refreshDisabledExporter(Exporter.Config exporterConf, ActionListener<ExporterResourceStatus> listener, int retries,
                                         TimeValue retryBackoff) {
        Exporter disabledExporter = exporters.openExporter(exporterConf);
        refreshOpenExporter(disabledExporter, new ActionListener<>() {
                @Override
                public void onResponse(ExporterResourceStatus exporterResourceStatus) {
                    disabledExporter.close();
                    listener.onResponse(exporterResourceStatus);
                }

                @Override
                public void onFailure(Exception e) {
                    disabledExporter.close();
                    listener.onFailure(e);
                }
            }, retries, retryBackoff);
    }

    @Override
    protected ClusterBlockException checkBlock(MonitoringMigrateAlertsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
