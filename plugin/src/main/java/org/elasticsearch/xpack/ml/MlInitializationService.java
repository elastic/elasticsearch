/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.retention.ExpiredModelSnapshotsRemover;
import org.elasticsearch.xpack.ml.job.retention.ExpiredResultsRemover;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class MlInitializationService extends AbstractComponent implements ClusterStateListener {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;
    private final JobProvider jobProvider;
    private final Auditor auditor;

    private final AtomicBoolean installMlMetadataCheck = new AtomicBoolean(false);
    private final AtomicBoolean putMlNotificationsIndexTemplateCheck = new AtomicBoolean(false);
    private final AtomicBoolean putMlMetaIndexTemplateCheck = new AtomicBoolean(false);
    private final AtomicBoolean putStateIndexTemplateCheck = new AtomicBoolean(false);
    private final AtomicBoolean putResultsIndexTemplateCheck = new AtomicBoolean(false);

    private volatile MlDailyManagementService mlDailyManagementService;

    public MlInitializationService(Settings settings, ThreadPool threadPool, ClusterService clusterService, Client client,
                                   JobProvider jobProvider, Auditor auditor) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.jobProvider = jobProvider;
        this.auditor = auditor;
        clusterService.addListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                super.beforeStop();
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            MetaData metaData = event.state().metaData();
            installMlMetadata(metaData);
            putMlNoficationsIndexTemplate(metaData);
            putMlMetaIndexTemplate(metaData);
            putStateIndexTemplate(metaData);
            putResultsIndexTemplate(metaData);
            installDailyManagementService();
        } else {
            uninstallDailyManagementService();
        }
    }

    private void installMlMetadata(MetaData metaData) {
        if (metaData.custom(MlMetadata.TYPE) == null) {
            if (installMlMetadataCheck.compareAndSet(false, true)) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    clusterService.submitStateUpdateTask("install-ml-metadata", new ClusterStateUpdateTask() {
                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            ClusterState.Builder builder = new ClusterState.Builder(currentState);
                            MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                            metadataBuilder.putCustom(MlMetadata.TYPE, MlMetadata.EMPTY_METADATA);
                            builder.metaData(metadataBuilder.build());
                            return builder.build();
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.error("unable to install ml metadata upon startup", e);
                        }
                    });
                });
            }
        } else {
            installMlMetadataCheck.set(false);
        }
    }

    private void putMlNoficationsIndexTemplate(MetaData metaData) {
        if (metaData.templates().containsKey(Auditor.NOTIFICATIONS_INDEX) == false) {
            if (putMlNotificationsIndexTemplateCheck.compareAndSet(false, true)) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    jobProvider.putNotificationMessageIndexTemplate((result, error) -> {
                        if (result) {
                            logger.info("successfully created {} index template", Auditor.NOTIFICATIONS_INDEX);
                        } else {
                            logger.error(
                                    new ParameterizedMessage("not able to create {} index template", Auditor.NOTIFICATIONS_INDEX), error);
                        }
                    });
                });
            } else {
                putMlNotificationsIndexTemplateCheck.set(false);
            }
        }
    }

    private void putMlMetaIndexTemplate(MetaData metaData) {
        if (metaData.templates().containsKey(JobProvider.ML_META_INDEX) == false) {
            if (putMlMetaIndexTemplateCheck.compareAndSet(false, true)) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    jobProvider.putMetaIndexTemplate((result, error) -> {
                        if (result) {
                            logger.info("successfully created {} index template", JobProvider.ML_META_INDEX);
                        } else {
                            logger.error(
                                new ParameterizedMessage("not able to create {} index template", JobProvider.ML_META_INDEX), error);
                        }
                    });
                });
            } else {
                putMlMetaIndexTemplateCheck.set(false);
            }

        }
    }

    private void putStateIndexTemplate(MetaData metaData) {
        String stateIndexName = AnomalyDetectorsIndex.jobStateIndexName();
        if (metaData.templates().containsKey(stateIndexName) == false) {
            if (putStateIndexTemplateCheck.compareAndSet(false, true)) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    jobProvider.putJobStateIndexTemplate((result, error) -> {
                        if (result) {
                            logger.info("successfully created {} index template", stateIndexName);
                        } else {
                            logger.error("not able to create " + stateIndexName + " index template", error);
                        }
                    });
                });
            } else {
                putStateIndexTemplateCheck.set(false);
            }
        }
    }

    private void putResultsIndexTemplate(MetaData metaData) {
        if (metaData.templates().containsKey(AnomalyDetectorsIndex.jobResultsIndexPrefix()) == false) {
            if (putResultsIndexTemplateCheck.compareAndSet(false, true)) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    jobProvider.putJobResultsIndexTemplate((result, error) -> {
                        if (result) {
                            logger.info("successfully created {} index template", AnomalyDetectorsIndex.jobResultsIndexPrefix());
                        } else {
                            logger.error(
                                    new ParameterizedMessage("not able to create {} index template",
                                            AnomalyDetectorsIndex.jobResultsIndexPrefix()), error);
                        }
                    });
                });
            } else {
                putResultsIndexTemplateCheck.set(false);
            }
        }
    }

    private void installDailyManagementService() {
        if (mlDailyManagementService == null) {
            mlDailyManagementService = new MlDailyManagementService(threadPool, Arrays.asList((MlDailyManagementService.Listener)
                    new ExpiredResultsRemover(client, clusterService, auditor),
                    new ExpiredModelSnapshotsRemover(client, clusterService)
            ));
            mlDailyManagementService.start();
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    uninstallDailyManagementService();
                }
            });
        }
    }

    private void uninstallDailyManagementService() {
        if (mlDailyManagementService != null) {
            mlDailyManagementService.stop();
            mlDailyManagementService = null;
        }
    }

    /** For testing */
    MlDailyManagementService getDailyManagementService() {
        return mlDailyManagementService;
    }

    /** For testing */
    void setDailyManagementService(MlDailyManagementService service) {
        mlDailyManagementService = service;
    }
}

