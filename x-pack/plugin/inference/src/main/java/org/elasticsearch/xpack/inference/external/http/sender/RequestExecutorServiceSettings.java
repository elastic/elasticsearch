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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    /**
     * Defines how often all the rate limit groups are polled for tasks. Setting this to very low number could result
     * in a busy loop if there are no tasks available to handle.
     */
    static final Setting<TimeValue> TASK_POLL_FREQUENCY_SETTING = Setting.timeSetting(
        "xpack.inference.http.request_executor.task_poll_frequency",
        DEFAULT_TASK_POLL_FREQUENCY_TIME,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_RATE_LIMIT_GROUP_CLEANUP_INTERVAL = TimeValue.timeValueDays(1);
    /**
     * Defines how often a thread will check for rate limit groups that are stale.
     */
    static final Setting<TimeValue> RATE_LIMIT_GROUP_CLEANUP_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.inference.http.request_executor.rate_limit_group_cleanup_interval",
        DEFAULT_RATE_LIMIT_GROUP_CLEANUP_INTERVAL,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_RATE_LIMIT_GROUP_STALE_DURATION = TimeValue.timeValueDays(10);
    /**
     * Defines the amount of time it takes to classify a rate limit group as stale. Once it is classified as stale,
     * it can be removed when the cleanup thread executes.
     */
    static final Setting<TimeValue> RATE_LIMIT_GROUP_STALE_DURATION_SETTING = Setting.timeSetting(
        "xpack.inference.http.request_executor.rate_limit_group_stale_duration",
        DEFAULT_RATE_LIMIT_GROUP_STALE_DURATION,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(
            TASK_QUEUE_CAPACITY_SETTING,
            TASK_POLL_FREQUENCY_SETTING,
            RATE_LIMIT_GROUP_CLEANUP_INTERVAL_SETTING,
            RATE_LIMIT_GROUP_STALE_DURATION_SETTING
        );
    }

    private volatile int queueCapacity;
    private volatile TimeValue taskPollFrequency;
    private volatile Duration rateLimitGroupStaleDuration;
    private final ConcurrentMap<String, Consumer<Integer>> queueCapacityCallbacks = new ConcurrentHashMap<>();

    public RequestExecutorServiceSettings(Settings settings, ClusterService clusterService) {
        queueCapacity = TASK_QUEUE_CAPACITY_SETTING.get(settings);
        taskPollFrequency = TASK_POLL_FREQUENCY_SETTING.get(settings);
        setRateLimitGroupStaleDuration(RATE_LIMIT_GROUP_STALE_DURATION_SETTING.get(settings));

        addSettingsUpdateConsumers(clusterService);
    }

    private void addSettingsUpdateConsumers(ClusterService clusterService) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TASK_QUEUE_CAPACITY_SETTING, this::setQueueCapacity);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TASK_POLL_FREQUENCY_SETTING, this::setTaskPollFrequency);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(RATE_LIMIT_GROUP_STALE_DURATION_SETTING, this::setRateLimitGroupStaleDuration);
    }

    // default for testing
    void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;

        for (var callback : queueCapacityCallbacks.values()) {
            callback.accept(queueCapacity);
        }
    }

    private void setTaskPollFrequency(TimeValue taskPollFrequency) {
        this.taskPollFrequency = taskPollFrequency;
    }

    private void setRateLimitGroupStaleDuration(TimeValue staleDuration) {
        rateLimitGroupStaleDuration = toDuration(staleDuration);
    }

    private static Duration toDuration(TimeValue timeValue) {
        return Duration.of(timeValue.duration(), timeValue.timeUnit().toChronoUnit());
    }

    void registerQueueCapacityCallback(String id, Consumer<Integer> onChangeCapacityCallback) {
        queueCapacityCallbacks.put(id, onChangeCapacityCallback);
    }

    void deregisterQueueCapacityCallback(String id) {
        queueCapacityCallbacks.remove(id);
    }

    int getQueueCapacity() {
        return queueCapacity;
    }

    TimeValue getTaskPollFrequency() {
        return taskPollFrequency;
    }

    Duration getRateLimitGroupStaleDuration() {
        return rateLimitGroupStaleDuration;
    }
}
