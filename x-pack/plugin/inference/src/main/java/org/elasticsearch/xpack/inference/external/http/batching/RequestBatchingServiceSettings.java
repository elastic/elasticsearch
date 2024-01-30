/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class RequestBatchingServiceSettings {

    /**
     * The amount of time to wait before sending a batched request. If the batch execution threshold amount is reached
     * before this time elapses, the batch will be sent. When this time amount elapses and the threshold has not been reached, the
     * batched requests will be sent anyway.
     */
    static final Setting<TimeValue> BATCHING_WAIT_PERIOD_SETTING = Setting.timeSetting(
        "xpack.inference.http.batching.request_executor.batching_wait_period",
        TimeValue.timeValueMillis(50),
        // TODO should we just allow 0? That would result in a tight loop but 1 probably does too
        TimeValue.timeValueNanos(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * A size dictating when the service should batch and send the buffered requests. This doesn't necessarily equate to the
     * size of a batch that is sent. If requests are buffered with different API Keys then they won't be batched together but they
     * both contribute to reaching the execution threshold.
     *
     * Using a value of one here will result in each request being sent once received.
     */
    static final Setting<Integer> BATCH_EXECUTION_THRESHOLD_SETTING = Setting.intSetting(
        "xpack.inference.http.batching.request_executor.batch_execution_threshold",
        100,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The capacity of the internal queue. Zero is considered unlimited. If a positive value is used, the queue will reject entries
     * once it is full.
     */
    static final Setting<Integer> TASK_QUEUE_CAPACITY_SETTING = Setting.intSetting(
        "xpack.inference.http.batching.request_executor.queue_capacity",
        50,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(BATCHING_WAIT_PERIOD_SETTING, BATCH_EXECUTION_THRESHOLD_SETTING, TASK_QUEUE_CAPACITY_SETTING);
    }

    private volatile TimeValue batchingWaitPeriod;
    private volatile int batchExecutionThreshold;
    private volatile int queueCapacity;
    private final List<Consumer<Integer>> queueCapacityCallbacks = new ArrayList<Consumer<Integer>>();

    public RequestBatchingServiceSettings(Settings settings, ClusterService clusterService) {
        batchingWaitPeriod = BATCHING_WAIT_PERIOD_SETTING.get(settings);
        batchExecutionThreshold = BATCH_EXECUTION_THRESHOLD_SETTING.get(settings);
        queueCapacity = TASK_QUEUE_CAPACITY_SETTING.get(settings);

        addSettingsUpdateConsumers(clusterService);
    }

    private void addSettingsUpdateConsumers(ClusterService clusterService) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCHING_WAIT_PERIOD_SETTING, this::setBatchingWaitPeriod);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_EXECUTION_THRESHOLD_SETTING, this::setBatchExecutionThreshold);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TASK_QUEUE_CAPACITY_SETTING, this::setQueueCapacity);
    }

    private void setBatchingWaitPeriod(TimeValue timeout) {
        batchingWaitPeriod = timeout;
    }

    private void setBatchExecutionThreshold(int batchSize) {
        batchExecutionThreshold = batchSize;
    }

    // default for testing
    void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;

        for (var callback : queueCapacityCallbacks) {
            callback.accept(queueCapacity);
        }
    }

    void registerQueueCapacityCallback(Consumer<Integer> onChangeCapacityCallback) {
        queueCapacityCallbacks.add(onChangeCapacityCallback);
    }

    TimeValue getBatchingWaitPeriod() {
        return batchingWaitPeriod;
    }

    int getBatchExecutionThreshold() {
        return batchExecutionThreshold;
    }

    int getQueueCapacity() {
        return queueCapacity;
    }
}
