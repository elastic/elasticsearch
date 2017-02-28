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
import org.elasticsearch.xpack.ml.job.retention.ExpiredModelSnapshotsRemover;
import org.elasticsearch.xpack.ml.job.retention.ExpiredResultsRemover;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class MlInitializationService extends AbstractComponent implements ClusterStateListener {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;
    private final Auditor auditor;

    private final AtomicBoolean installMlMetadataCheck = new AtomicBoolean(false);
    private volatile MlDailyManagementService mlDailyManagementService;

    public MlInitializationService(Settings settings, ThreadPool threadPool, ClusterService clusterService, Client client,
                                   Auditor auditor) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
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

