/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.common.annotation.PublicApi;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This class does throttling on task submission to cluster manager node, it uses throttling key defined in various executors
 * as key for throttling. Throttling will be performed over task executor's class level, different task types have different executors class.
 * <p>
 * Set specific setting to for setting the threshold of throttling of particular task type.
 * e.g : Set "cluster_manager.throttling.thresholds.put_mapping" to set throttling limit of "put mapping" tasks,
 *       Set it to default value(-1) to disable the throttling for this task type.
 */
public abstract class ClusterManagerTaskThrottler implements TaskBatcherListener {
    private static final Logger logger = LogManager.getLogger(ClusterManagerTaskThrottler.class);
    public static final ThrottlingKey DEFAULT_THROTTLING_KEY = new ThrottlingKey("default-task-key", false);

    // default value for base delay is 5s
    static volatile TimeValue baseDelay = TimeValue.timeValueSeconds(5);
    // default values for max delay is 30s
    static volatile TimeValue maxDelay = TimeValue.timeValueSeconds(30);

    public static final Setting<Settings> THRESHOLD_SETTINGS = Setting.groupSetting(
        "cluster_manager.throttling.thresholds.",
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> BASE_DELAY_SETTINGS = Setting.timeSetting(
        "cluster_manager.throttling.retry.base.delay",
        baseDelay,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MAX_DELAY_SETTINGS = Setting.timeSetting(
        "cluster_manager.throttling.retry.max.delay",
        maxDelay,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    protected Map<String, ThrottlingKey> THROTTLING_TASK_KEYS = new ConcurrentHashMap<>();

    private final int MIN_THRESHOLD_VALUE = -1; // Disabled throttling
    private final ClusterManagerTaskThrottlerListener clusterManagerTaskThrottlerListener;

    private final ConcurrentMap<String, Long> tasksCount;
    private final ConcurrentMap<String, Long> tasksThreshold;
    private final Supplier<Version> minNodeVersionSupplier;

    // Once all nodes are greater than or equal 2.5.0 version, then only it will start throttling.
    // During upgrade as well, it will wait for all older version nodes to leave the cluster before starting throttling.
    // This is needed specifically for static setting to enable throttling.
    private AtomicBoolean startThrottling = new AtomicBoolean();

    public ClusterManagerTaskThrottler(
        final Settings settings,
        final ClusterSettings clusterSettings,
        final Supplier<Version> minNodeVersionSupplier,
        final ClusterManagerTaskThrottlerListener clusterManagerTaskThrottlerListener
    ) {
        tasksCount = new ConcurrentHashMap<>(128); // setting initial capacity so each task will land in different segment
        tasksThreshold = new ConcurrentHashMap<>(128); // setting initial capacity so each task will land in different segment
        this.minNodeVersionSupplier = minNodeVersionSupplier;
        this.clusterManagerTaskThrottlerListener = clusterManagerTaskThrottlerListener;
        clusterSettings.addSettingsUpdateConsumer(THRESHOLD_SETTINGS, this::updateSetting, this::validateSetting);
        clusterSettings.addSettingsUpdateConsumer(BASE_DELAY_SETTINGS, this::updateBaseDelay);
        clusterSettings.addSettingsUpdateConsumer(MAX_DELAY_SETTINGS, this::updateMaxDelay);
        // Required for setting values as per current settings during node bootstrap
        updateSetting(THRESHOLD_SETTINGS.get(settings));
        updateBaseDelay(BASE_DELAY_SETTINGS.get(settings));
        updateMaxDelay(MAX_DELAY_SETTINGS.get(settings));
    }

    void updateBaseDelay(TimeValue newBaseValue) {
        baseDelay = newBaseValue;
    }

    void updateMaxDelay(TimeValue newMaxValue) {
        maxDelay = newMaxValue;
    }

    public static TimeValue getBaseDelayForRetry() {
        return baseDelay;
    }

    public static TimeValue getMaxDelayForRetry() {
        return maxDelay;
    }

    /**
     * To configure a new task for throttling,
     * * Register task to cluster service with task key,
     * * override getClusterManagerThrottlingKey method with above task key in task executor.
     * * Verify that throttled tasks would be retried from
     * data nodes
     * <p>
     * Added retry mechanism in TransportClusterManagerNodeAction, so it would be retried for customer generated tasks.
     * <p>
     * If tasks are not getting retried then we can register with false flag, so user won't be able to configure threshold limits for it.
     */
    protected ThrottlingKey registerClusterManagerTask(String taskKey, boolean throttlingEnabled) {
        ThrottlingKey throttlingKey = new ThrottlingKey(taskKey, throttlingEnabled);
        if (THROTTLING_TASK_KEYS.containsKey(taskKey)) {
            throw new IllegalArgumentException("There is already a Throttling key registered with same name: " + taskKey);
        }
        THROTTLING_TASK_KEYS.put(taskKey, throttlingKey);
        return throttlingKey;
    }

    /**
     * Class to store the throttling key for the tasks of cluster manager
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class ThrottlingKey {
        private String taskThrottlingKey;
        private boolean throttlingEnabled;

        /**
         * Class for throttling key of tasks
         *
         * @param taskThrottlingKey - throttling key for task
         * @param throttlingEnabled - if throttling is enabled or not i.e. data node is performing retry over throttling exception or not.
         */
        private ThrottlingKey(String taskThrottlingKey, boolean throttlingEnabled) {
            this.taskThrottlingKey = taskThrottlingKey;
            this.throttlingEnabled = throttlingEnabled;
        }

        public String getTaskThrottlingKey() {
            return taskThrottlingKey;
        }

        public boolean isThrottlingEnabled() {
            return throttlingEnabled;
        }
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    void validateSetting(final Settings settings) {
        Map<String, Settings> groups = settings.getAsGroups();
        if (groups.size() > 0) {
            if (minNodeVersionSupplier.get().compareTo(Version.V_2_5_0) < 0) {
                throw new IllegalArgumentException("All the nodes in cluster should be on version later than or equal to 2.5.0");
            }
        }
        for (String key : groups.keySet()) {
            if (!THROTTLING_TASK_KEYS.containsKey(key)) {
                throw new IllegalArgumentException("Cluster manager task throttling is not configured for given task type: " + key);
            }
            if (!THROTTLING_TASK_KEYS.get(key).isThrottlingEnabled()) {
                throw new IllegalArgumentException("Throttling is not enabled for given task type: " + key);
            }
            int threshold = groups.get(key).getAsInt("value", MIN_THRESHOLD_VALUE);
            if (threshold < MIN_THRESHOLD_VALUE) {
                throw new IllegalArgumentException("Provide positive integer for limit or -1 for disabling throttling");
            }
        }
    }

    void updateSetting(final Settings newSettings) {
        Map<String, Settings> groups = newSettings.getAsGroups();
        Set<String> settingKeys = new HashSet<>();
        // Adding keys which are present in new Setting
        settingKeys.addAll(groups.keySet());
        // Adding existing keys that may need to be set to a default value if that is removed in new setting.
        settingKeys.addAll(tasksThreshold.keySet());
        for (String key : settingKeys) {
            Settings setting = groups.get(key);
            updateLimit(key, setting == null ? MIN_THRESHOLD_VALUE : setting.getAsInt("value", MIN_THRESHOLD_VALUE));
        }
    }

    void updateLimit(final String taskKey, final int limit) {
        assert limit >= MIN_THRESHOLD_VALUE;
        if (limit == MIN_THRESHOLD_VALUE) {
            tasksThreshold.remove(taskKey);
        } else {
            tasksThreshold.put(taskKey, (long) limit);
        }
    }

    Long getThrottlingLimit(final String taskKey) {
        return tasksThreshold.get(taskKey);
    }

    @Override
    public void onBeginSubmit(List<? extends TaskBatcher.BatchedTask> tasks) {
        ThrottlingKey clusterManagerThrottlingKey = ((ClusterStateTaskExecutor<Object>) tasks.get(0).batchingKey)
            .getClusterManagerThrottlingKey();
        tasksCount.putIfAbsent(clusterManagerThrottlingKey.getTaskThrottlingKey(), 0L);
        tasksCount.computeIfPresent(clusterManagerThrottlingKey.getTaskThrottlingKey(), (key, count) -> {
            int size = tasks.size();
            if (clusterManagerThrottlingKey.isThrottlingEnabled()) {
                Long threshold = tasksThreshold.get(clusterManagerThrottlingKey.getTaskThrottlingKey());
                if (threshold != null && shouldThrottle(threshold, count, size)) {
                    clusterManagerTaskThrottlerListener.onThrottle(clusterManagerThrottlingKey.getTaskThrottlingKey(), size);
                    logger.warn(
                        "Throwing Throttling Exception for [{}]. Trying to add [{}] tasks to queue, limit is set to [{}]",
                        clusterManagerThrottlingKey.getTaskThrottlingKey(),
                        tasks.size(),
                        threshold
                    );
                    throw new ClusterManagerThrottlingException(
                        "Throttling Exception : Limit exceeded for " + clusterManagerThrottlingKey.getTaskThrottlingKey()
                    );
                }
            }
            return count + size;
        });
    }

    /**
     * If throttling thresholds are set via static setting, it will update the threshold map.
     * It may start throwing throttling exception to older nodes in cluster.
     * Older version nodes will not be equipped to handle the throttling exception and
     * this may result in unexpected behavior where internal tasks would start failing without any retries.
     * <p>
     * For every task submission request, it will validate if nodes version is greater or equal to 2.5.0 and set the startThrottling flag.
     * Once the startThrottling flag is set, it will not perform check for next set of tasks.
     */
    @SuppressWarnings("checkstyle:DescendantToken")
    private boolean shouldThrottle(Long threshold, Long count, int size) {
        if (!startThrottling.get()) {
            if (minNodeVersionSupplier.get().compareTo(Version.V_2_5_0) >= 0) {
                startThrottling.compareAndSet(false, true);
                logger.info("Starting cluster manager throttling as all nodes are higher than or equal to 2.5.0");
            } else {
                logger.info("Skipping cluster manager throttling as at least one node < 2.5.0 is present in cluster");
                return false;
            }
        }
        return count + size > threshold;
    }

    @Override
    public void onSubmitFailure(List<? extends TaskBatcher.BatchedTask> tasks) {
        reduceTaskCount(tasks);
    }

    /**
     * Tasks will be removed from the queue before processing, so here we will reduce the count of tasks.
     *
     * @param tasks list of tasks which will be executed.
     */
    @Override
    public void onBeginProcessing(List<? extends TaskBatcher.BatchedTask> tasks) {
        reduceTaskCount(tasks);
    }

    @Override
    public void onTimeout(List<? extends TaskBatcher.BatchedTask> tasks) {
        reduceTaskCount(tasks);
    }

    private void reduceTaskCount(List<? extends TaskBatcher.BatchedTask> tasks) {
        String masterTaskKey = ((ClusterStateTaskExecutor<Object>) tasks.get(0).batchingKey).getClusterManagerThrottlingKey()
            .getTaskThrottlingKey();
        tasksCount.computeIfPresent(masterTaskKey, (key, count) -> count - tasks.size());
    }
}
