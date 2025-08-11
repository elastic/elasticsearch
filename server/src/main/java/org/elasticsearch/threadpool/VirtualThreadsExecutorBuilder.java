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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.Node;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VirtualThreadsExecutorBuilder extends ExecutorBuilder<VirtualThreadsExecutorSettings> {

    private static final Logger logger = LogManager.getLogger(VirtualThreadsExecutorBuilder.class);

    private final boolean rejectAfterShutdown;
    private final List<Setting<?>> registeredSettings;

    public VirtualThreadsExecutorBuilder(String name, boolean rejectAfterShutdown) {
        super(name, false);
        this.rejectAfterShutdown = rejectAfterShutdown;
        final String settingsPrefix = "thread_pool." + name + ".";
        // Don't apply any of these settings, just define them to make test failures go away
        registeredSettings = List.of(
            Setting.intSetting(settingsPrefix + "size", 999, 1, Setting.Property.NodeScope),
            Setting.intSetting(settingsPrefix + "max", 999, 1, Setting.Property.NodeScope),
            Setting.intSetting(settingsPrefix + "core", 999, 0, Setting.Property.NodeScope),
            Setting.intSetting(settingsPrefix + "queue_size", 999, Setting.Property.NodeScope)
        );
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return registeredSettings;
    }

    @Override
    VirtualThreadsExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        for (Setting<?> setting : registeredSettings) {
            if (setting.exists(settings)) {
                logger.warn("Ignoring setting [{}] for virtual thread pool [{}]", setting, name());
            }
        }
        return new VirtualThreadsExecutorSettings(nodeName);
    }

    @Override
    ThreadPool.ExecutorHolder build(VirtualThreadsExecutorSettings settings, ThreadContext threadContext) {
        final String threadName = EsExecutors.threadName(settings.nodeName, name());
        final ExecutorService executorService = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(threadName + "[v]").factory());
        return new ThreadPool.ExecutorHolder(
            new EsExecutorServiceDecorator(name(), executorService, threadContext, rejectAfterShutdown),
            new ThreadPool.Info(name(), ThreadPool.ThreadPoolType.VIRTUAL, 999, 999, TimeValue.ZERO, SizeValue.parseSizeValue("999"))
        );
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(Locale.ROOT, "name [%s], virtual", info.getName());
    }
}
