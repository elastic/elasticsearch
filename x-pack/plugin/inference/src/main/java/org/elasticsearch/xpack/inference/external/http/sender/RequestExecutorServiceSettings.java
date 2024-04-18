/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class RequestExecutorServiceSettings {

    /**
     * The capacity of the internal queue. Zero is considered unlimited. If a positive value is used, the queue will reject entries
     * once it is full.
     */
    static final Setting<Integer> TASK_QUEUE_CAPACITY_SETTING = Setting.intSetting(
        "xpack.inference.http.request_executor.queue_capacity",
        2000,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_TASK_POLL_FREQUENCY_TIME = TimeValue.timeValueMillis(50);
    static final Setting<TimeValue> TASK_POLL_FREQUENCY_SETTING = Setting.timeSetting(
        "xpack.inference.http.request_executor.task_poll_frequency",
        DEFAULT_TASK_POLL_FREQUENCY_TIME,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(TASK_QUEUE_CAPACITY_SETTING, TASK_POLL_FREQUENCY_SETTING);
    }

    private volatile int queueCapacity;
    private volatile TimeValue taskPollFrequency;
    private final List<Consumer<Integer>> queueCapacityCallbacks = new ArrayList<Consumer<Integer>>();

    public RequestExecutorServiceSettings(Settings settings, ClusterService clusterService) {
        queueCapacity = TASK_QUEUE_CAPACITY_SETTING.get(settings);
        taskPollFrequency = TASK_POLL_FREQUENCY_SETTING.get(settings);

        addSettingsUpdateConsumers(clusterService);
    }

    private void addSettingsUpdateConsumers(ClusterService clusterService) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TASK_QUEUE_CAPACITY_SETTING, this::setQueueCapacity);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TASK_POLL_FREQUENCY_SETTING, this::setTaskPollFrequency);
    }

    // default for testing
    void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;

        for (var callback : queueCapacityCallbacks) {
            callback.accept(queueCapacity);
        }
    }

    private void setTaskPollFrequency(TimeValue taskPollFrequency) {
        this.taskPollFrequency = taskPollFrequency;
    }

    void registerQueueCapacityCallback(Consumer<Integer> onChangeCapacityCallback) {
        queueCapacityCallbacks.add(onChangeCapacityCallback);
    }

    int getQueueCapacity() {
        return queueCapacity;
    }

    TimeValue getTaskPollFrequency() {
        return taskPollFrequency;
    }

}
