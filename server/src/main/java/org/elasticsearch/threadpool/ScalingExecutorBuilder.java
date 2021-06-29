/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A builder for scaling executors.
 */
public final class ScalingExecutorBuilder extends ExecutorBuilder<ScalingExecutorBuilder.ScalingExecutorSettings> {

    private final Setting<Integer> coreSetting;
    private final Setting<Integer> maxSetting;
    private final Setting<TimeValue> keepAliveSetting;

    /**
     * Construct a scaling executor builder; the settings will have the
     * key prefix "thread_pool." followed by the executor name.
     *
     * @param name      the name of the executor
     * @param core      the minimum number of threads in the pool
     * @param max       the maximum number of threads in the pool
     * @param keepAlive the time that spare threads above {@code core}
     *                  threads will be kept alive
     */
    public ScalingExecutorBuilder(final String name, final int core, final int max, final TimeValue keepAlive) {
        this(name, core, max, keepAlive, "thread_pool." + name);
    }

    /**
     * Construct a scaling executor builder; the settings will have the
     * specified key prefix.
     *
     * @param name      the name of the executor
     * @param core      the minimum number of threads in the pool
     * @param max       the maximum number of threads in the pool
     * @param keepAlive the time that spare threads above {@code core}
     *                  threads will be kept alive
     * @param prefix    the prefix for the settings keys
     */
    public ScalingExecutorBuilder(final String name, final int core, final int max, final TimeValue keepAlive, final String prefix) {
        super(name);
        this.coreSetting =
            Setting.intSetting(settingsKey(prefix, "core"), core, Setting.Property.NodeScope);
        this.maxSetting = Setting.intSetting(settingsKey(prefix, "max"), max, Setting.Property.NodeScope);
        this.keepAliveSetting =
            Setting.timeSetting(settingsKey(prefix, "keep_alive"), keepAlive, Setting.Property.NodeScope);
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Arrays.asList(coreSetting, maxSetting, keepAliveSetting);
    }

    @Override
    ScalingExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int coreThreads = coreSetting.get(settings);
        final int maxThreads = maxSetting.get(settings);
        final TimeValue keepAlive = keepAliveSetting.get(settings);
        return new ScalingExecutorSettings(nodeName, coreThreads, maxThreads, keepAlive);
    }

    ThreadPool.ExecutorHolder build(final ScalingExecutorSettings settings, final ThreadContext threadContext) {
        TimeValue keepAlive = settings.keepAlive;
        int core = settings.core;
        int max = settings.max;
        final ThreadPool.Info info = new ThreadPool.Info(name(), ThreadPool.ThreadPoolType.SCALING, core, max, keepAlive, null);
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName(settings.nodeName, name()));
        final ExecutorService executor =
            EsExecutors.newScaling(
                    settings.nodeName + "/" + name(),
                    core,
                    max,
                    keepAlive.millis(),
                    TimeUnit.MILLISECONDS,
                    threadFactory,
                    threadContext);
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], core [%d], max [%d], keep alive [%s]",
            info.getName(),
            info.getMin(),
            info.getMax(),
            info.getKeepAlive());
    }

    static class ScalingExecutorSettings extends ExecutorBuilder.ExecutorSettings {

        private final int core;
        private final int max;
        private final TimeValue keepAlive;

        ScalingExecutorSettings(final String nodeName, final int core, final int max, final TimeValue keepAlive) {
            super(nodeName);
            this.core = core;
            this.max = max;
            this.keepAlive = keepAlive;
        }
    }

}
