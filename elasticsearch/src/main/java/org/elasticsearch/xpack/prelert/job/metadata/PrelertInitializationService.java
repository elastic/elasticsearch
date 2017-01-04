/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;

import java.util.concurrent.atomic.AtomicBoolean;

public class PrelertInitializationService extends AbstractComponent implements ClusterStateListener {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final JobProvider jobProvider;

    private final AtomicBoolean installPrelertMetadataCheck = new AtomicBoolean(false);
    private final AtomicBoolean createPrelertUsageIndexCheck = new AtomicBoolean(false);
    private final AtomicBoolean createStateIndexCheck = new AtomicBoolean(false);

    public PrelertInitializationService(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                        JobProvider jobProvider) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.jobProvider = jobProvider;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            MetaData metaData = event.state().metaData();
            if (metaData.custom(PrelertMetadata.TYPE) == null) {
                if (installPrelertMetadataCheck.compareAndSet(false, true)) {
                    threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                        clusterService.submitStateUpdateTask("install-prelert-metadata", new ClusterStateUpdateTask() {
                            @Override
                            public ClusterState execute(ClusterState currentState) throws Exception {
                                ClusterState.Builder builder = new ClusterState.Builder(currentState);
                                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                                metadataBuilder.putCustom(PrelertMetadata.TYPE, PrelertMetadata.EMPTY_METADATA);
                                builder.metaData(metadataBuilder.build());
                                return builder.build();
                            }

                            @Override
                            public void onFailure(String source, Exception e) {
                                logger.error("unable to install prelert metadata upon startup", e);
                            }
                        });
                    });
                }
            } else {
                installPrelertMetadataCheck.set(false);
            }
            if (metaData.hasIndex(JobProvider.PRELERT_USAGE_INDEX) == false) {
                if (createPrelertUsageIndexCheck.compareAndSet(false, true)) {
                    threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                        jobProvider.createUsageMeteringIndex((result, error) -> {
                            if (result) {
                                logger.info("successfully created prelert-usage index");
                                createPrelertUsageIndexCheck.set(false);
                            } else {
                                if (error instanceof ResourceAlreadyExistsException) {
                                    logger.debug("not able to create prelert-usage index as it already exists");
                                } else {
                                    logger.error("not able to create prelert-usage index", error);
                                }
                            }
                        });
                    });
                }
            }
            String stateIndexName = AnomalyDetectorsIndex.jobStateIndexName();
            if (metaData.hasIndex(stateIndexName) == false) {
                if (createStateIndexCheck.compareAndSet(false, true)) {
                    threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                        jobProvider.createJobStateIndex((result, error) -> {
                            if (result) {
                                logger.info("successfully created {} index", stateIndexName);
                                createStateIndexCheck.set(false);
                            } else {
                                if (error instanceof ResourceAlreadyExistsException) {
                                    logger.debug("not able to create {} index as it already exists", stateIndexName);
                                } else {
                                    logger.error("not able to create " + stateIndexName + " index", error);
                                }
                            }
                        });
                    });
                }
            }
        }
    }
}
