/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * A builder for fixed executors.
 *
 * Builds an Executor with a static number of threads, as opposed to {@link ScalingExecutorBuilder} that dynamically scales the number of
 * threads in the pool up and down based on request load.
 */
public final class FixedExecutorBuilder extends ExecutorBuilder<FixedExecutorBuilder.FixedExecutorSettings> {

    private final Setting<Integer> sizeSetting;
    private final Setting<Integer> queueSizeSetting;
    private final TaskTrackingConfig taskTrackingConfig;

    /**
     * Construct a fixed executor builder; the settings will have the key prefix "thread_pool." followed by the executor name.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
     * @param taskTrackingConfig whether to track statics about task execution time
     */
    FixedExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final TaskTrackingConfig taskTrackingConfig
    ) {
        this(settings, name, size, queueSize, "thread_pool." + name, taskTrackingConfig, false);
    }

    /**
     * Construct a fixed executor builder; the settings will have the key prefix "thread_pool." followed by the executor name.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
     * @param taskTrackingConfig whether to track statics about task execution time
     * @param isSystemThread whether the threads are system threads
     */
    FixedExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final TaskTrackingConfig taskTrackingConfig,
        boolean isSystemThread
    ) {
        this(settings, name, size, queueSize, "thread_pool." + name, taskTrackingConfig, isSystemThread);
    }

    /**
     * Construct a fixed executor builder.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
     * @param prefix    the prefix for the settings keys
     * @param taskTrackingConfig whether to track statics about task execution time
     */
    public FixedExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final String prefix,
        final TaskTrackingConfig taskTrackingConfig
    ) {
        this(settings, name, size, queueSize, prefix, taskTrackingConfig, false);
    }

    /**
     * Construct a fixed executor builder.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
     * @param prefix    the prefix for the settings keys
     * @param taskTrackingConfig whether to track statics about task execution time
     */
    public FixedExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final String prefix,
        final TaskTrackingConfig taskTrackingConfig,
        final boolean isSystemThread
    ) {
        super(name, isSystemThread);
        final String sizeKey = settingsKey(prefix, "size");
        this.sizeSetting = new Setting<>(
            sizeKey,
            Integer.toString(size),
            s -> Setting.parseInt(s, 1, applyHardSizeLimit(settings, name), sizeKey),
            Setting.Property.NodeScope
        );
        final String queueSizeKey = settingsKey(prefix, "queue_size");
        this.queueSizeSetting = Setting.intSetting(queueSizeKey, queueSize, Setting.Property.NodeScope);
        this.taskTrackingConfig = taskTrackingConfig;
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Arrays.asList(sizeSetting, queueSizeSetting);
    }

    @Override
    FixedExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int size = sizeSetting.get(settings);
        final int queueSize = queueSizeSetting.get(settings);
        return new FixedExecutorSettings(nodeName, size, queueSize);
    }

    @Override
    ThreadPool.ExecutorHolder build(final FixedExecutorSettings settings, final ThreadContext threadContext) {
        int size = settings.size;
        int queueSize = settings.queueSize;
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings.nodeName, name(), isSystemThread());
        final ExecutorService executor = EsExecutors.newFixed(
            settings.nodeName + "/" + name(),
            size,
            queueSize,
            threadFactory,
            threadContext,
            taskTrackingConfig
        );
        final ThreadPool.Info info = new ThreadPool.Info(
            name(),
            ThreadPool.ThreadPoolType.FIXED,
            size,
            size,
            null,
            queueSize < 0 ? null : new SizeValue(queueSize)
        );
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], size [%d], queue size [%s]",
            info.getName(),
            info.getMax(),
            info.getQueueSize() == null ? "unbounded" : info.getQueueSize()
        );
    }

    static class FixedExecutorSettings extends ExecutorBuilder.ExecutorSettings {

        private final int size;
        private final int queueSize;

        FixedExecutorSettings(final String nodeName, final int size, final int queueSize) {
            super(nodeName);
            this.size = size;
            this.queueSize = queueSize;
        }

    }

}
