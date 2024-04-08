/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.health;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.health.node.DataStreamLifecycleHealthInfo;
import org.elasticsearch.health.node.DslErrorInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.health.node.selection.HealthNode;

import java.util.List;

import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING;

/**
 * Provides the infrastructure to send errors encountered by indices managed by data stream lifecycle service to the health node.
 */
public class DataStreamLifecycleHealthInfoPublisher {
    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleHealthInfoPublisher.class);
    /**
     * Controls the number of DSL error entries we publish to the health node.
     */
    public static final Setting<Integer> DATA_STREAM_LIFECYCLE_MAX_ERRORS_TO_PUBLISH_SETTING = Setting.intSetting(
        "data_streams.lifecycle.max_errors_to_publish",
        500,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final NodeFeature DSL_HEALTH_INFO_FEATURE = new NodeFeature("health.dsl.info");

    private final Client client;
    private final ClusterService clusterService;
    private final DataStreamLifecycleErrorStore errorStore;
    private final FeatureService featureService;
    private volatile int signallingErrorRetryInterval;
    private volatile int maxNumberOfErrorsToPublish;

    public DataStreamLifecycleHealthInfoPublisher(
        Settings settings,
        Client client,
        ClusterService clusterService,
        DataStreamLifecycleErrorStore errorStore,
        FeatureService featureService
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.errorStore = errorStore;
        this.featureService = featureService;
        this.signallingErrorRetryInterval = DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING.get(settings);
        this.maxNumberOfErrorsToPublish = DATA_STREAM_LIFECYCLE_MAX_ERRORS_TO_PUBLISH_SETTING.get(settings);
    }

    public void init() {
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING, this::updateSignallingRetryThreshold);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAM_LIFECYCLE_MAX_ERRORS_TO_PUBLISH_SETTING, this::updateNumberOfErrorsToPublish);
    }

    private void updateSignallingRetryThreshold(int newValue) {
        this.signallingErrorRetryInterval = newValue;
    }

    private void updateNumberOfErrorsToPublish(int newValue) {
        this.maxNumberOfErrorsToPublish = newValue;
    }

    /**
     * Publishes the DSL errors that have passed the signaling threshold (as defined by
     * {@link org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService#DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING}
     */
    public void publishDslErrorEntries(ActionListener<AcknowledgedResponse> actionListener) {
        if (featureService.clusterHasFeature(clusterService.state(), DSL_HEALTH_INFO_FEATURE) == false) {
            return;
        }
        // fetching the entries that persist in the error store for more than the signalling retry interval
        // note that we're reporting this view into the error store on every publishing iteration
        List<DslErrorInfo> errorEntriesToSignal = errorStore.getErrorsInfo(
            entry -> entry.retryCount() >= signallingErrorRetryInterval,
            maxNumberOfErrorsToPublish
        );
        DiscoveryNode currentHealthNode = HealthNode.findHealthNode(clusterService.state());
        if (currentHealthNode != null) {
            String healthNodeId = currentHealthNode.getId();
            logger.trace("reporting [{}] DSL error entries to to health node [{}]", errorEntriesToSignal.size(), healthNodeId);
            client.execute(
                UpdateHealthInfoCacheAction.INSTANCE,
                new UpdateHealthInfoCacheAction.Request(
                    healthNodeId,
                    new DataStreamLifecycleHealthInfo(errorEntriesToSignal, errorStore.getAllIndices().size())
                ),
                actionListener
            );
        } else {
            logger.trace("unable to report DSL health because there is no health node in the cluster. will retry on the next DSL run");
        }
    }
}
