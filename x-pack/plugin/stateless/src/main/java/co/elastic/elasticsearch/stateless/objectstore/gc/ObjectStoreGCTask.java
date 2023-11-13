/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.objectstore.gc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

public class ObjectStoreGCTask extends AllocatedPersistentTask {
    public static final Setting<Boolean> STALE_INDICES_GC_ENABLED_SETTING = Setting.boolSetting(
        "serverless.object_store.gc.stale_indices.enabled",
        true,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> STALE_TRANSLOGS_GC_ENABLED_SETTING = Setting.boolSetting(
        "serverless.object_store.gc.stale_translogs.enabled",
        true,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> STALE_TRANSLOGS_GC_FILES_LIMIT_SETTING = Setting.intSetting(
        "serverless.object_store.gc.stale_translogs.files_limit",
        100000,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GC_INTERVAL_SETTING = Setting.timeSetting(
        "serverless.object_store.gc.interval",
        TimeValue.timeValueHours(8),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );
    public static final String TASK_NAME = "object_store_gc";

    private final StaleIndicesGCService staleIndicesGCService;
    private final StaleTranslogsGCService staleTranslogsGCService;
    private final ThreadPool threadPool;
    private final boolean staleIndicesGCEnabled;
    private final boolean staleTranslogsGCEnabled;
    private final TimeValue interval;

    public ObjectStoreGCTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers,
        StaleIndicesGCService staleIndicesGCService,
        StaleTranslogsGCService staleTranslogsGCService,
        ThreadPool threadPool,
        Settings settings
    ) {
        super(id, type, action, description, parentTask, headers);
        this.staleIndicesGCService = staleIndicesGCService;
        this.staleTranslogsGCService = staleTranslogsGCService;
        this.threadPool = threadPool;
        this.staleIndicesGCEnabled = STALE_INDICES_GC_ENABLED_SETTING.get(settings);
        this.staleTranslogsGCEnabled = STALE_TRANSLOGS_GC_ENABLED_SETTING.get(settings);
        this.interval = GC_INTERVAL_SETTING.get(settings);
    }

    public void runGC() {
        if (isCancelled()) {
            return;
        }

        try (var listeners = new RefCountingListener(ActionListener.running(this::rescheduleGC))) {
            cleanStaleIndices(listeners.acquire());
            cleanStaleTranslogs(listeners.acquire());
        }
    }

    private void cleanStaleIndices(ActionListener<Void> listener) {
        if (staleIndicesGCEnabled == false || isCancelled()) {
            listener.onResponse(null);
            return;
        }

        staleIndicesGCService.cleanStaleIndices(listener);
    }

    private void cleanStaleTranslogs(ActionListener<Void> listener) {
        if (staleTranslogsGCEnabled == false || isCancelled()) {
            listener.onResponse(null);
            return;
        }

        staleTranslogsGCService.cleanStaleTranslogs(listener, ActionListener.noop());
    }

    // package private for testing
    void cleanStaleTranslogs(ActionListener<Void> listener, ActionListener<Void> listenerOnConsistentClusterState) {
        if (staleTranslogsGCEnabled == false || isCancelled()) {
            listener.onResponse(null);
            return;
        }

        staleTranslogsGCService.cleanStaleTranslogs(listener, listenerOnConsistentClusterState);
    }

    private void rescheduleGC() {
        if (isCancelled()) {
            return;
        }

        threadPool.scheduleUnlessShuttingDown(interval, threadPool.generic(), this::runGC);
    }

    @Override
    protected void onCancelled() {
        markAsCompleted();
    }
}
