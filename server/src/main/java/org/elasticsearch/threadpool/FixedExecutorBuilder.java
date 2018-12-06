/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * A builder for fixed executors.
 */
public final class FixedExecutorBuilder extends ExecutorBuilder<FixedExecutorBuilder.FixedExecutorSettings> {

    private final Setting<Integer> sizeSetting;
    private final Setting<Integer> fallbackSizeSetting;
    private final Setting<Integer> queueSizeSetting;
    private final Setting<Integer> fallbackQueueSizeSetting;

    /**
     * Construct a fixed executor builder; the settings will have the key prefix "thread_pool." followed by the executor name.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
     */
    FixedExecutorBuilder(final Settings settings, final String name, final int size, final int queueSize) {
        this(settings, name, size, queueSize, false);
    }

    /**
     * Construct a fixed executor builder; the settings will have the key prefix "thread_pool." followed by the executor name.
     *
     * @param settings     the node-level settings
     * @param name         the name of the executor
     * @param fallbackName the fallback name of the executor (used for transitioning the name of a setting)
     * @param size         the fixed number of threads
     * @param queueSize    the size of the backing queue, -1 for unbounded
     */
    FixedExecutorBuilder(final Settings settings, final String name, final String fallbackName, final int size, final int queueSize) {
        this(settings, name, fallbackName, size, queueSize, "thread_pool." + name, "thread_pool." + fallbackName, false);
    }

    /**
     * Construct a fixed executor builder; the settings will have the key prefix "thread_pool." followed by the executor name.
     *
     * @param settings   the node-level settings
     * @param name       the name of the executor
     * @param size       the fixed number of threads
     * @param queueSize  the size of the backing queue, -1 for unbounded
     * @param deprecated whether or not the thread pool is deprecated
     */
    FixedExecutorBuilder(final Settings settings, final String name, final int size, final int queueSize, final boolean deprecated) {
        this(settings, name, null, size, queueSize, "thread_pool." + name, null, deprecated);
    }

    /**
     * Construct a fixed executor builder.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
     * @param prefix    the prefix for the settings keys
     */
    public FixedExecutorBuilder(final Settings settings, final String name, final int size, final int queueSize, final String prefix) {
        this(settings, name, null, size, queueSize, prefix, null, false);
    }

    /**
     * Construct a fixed executor builder.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
     * @param prefix    the prefix for the settings keys
     */
    private FixedExecutorBuilder(
            final Settings settings,
            final String name,
            final String fallbackName,
            final int size,
            final int queueSize,
            final String prefix,
            final String fallbackPrefix,
            final boolean deprecated) {
        super(name);
        final String sizeKey = settingsKey(prefix, "size");
        final String queueSizeKey = settingsKey(prefix, "queue_size");
        if (fallbackName == null) {
            final Setting.Property[] properties;
            if (deprecated) {
                properties = new Setting.Property[]{Setting.Property.NodeScope, Setting.Property.Deprecated};
            } else {
                properties = new Setting.Property[]{Setting.Property.NodeScope};
            }
            assert fallbackPrefix == null;
            this.sizeSetting = sizeSetting(settings, name, size, prefix, properties);
            this.fallbackSizeSetting = null;
            this.queueSizeSetting = queueSizeSetting(prefix, queueSize, properties);
            this.fallbackQueueSizeSetting = null;
        } else {
            assert fallbackPrefix != null;
            assert deprecated == false;
            final Setting.Property[] properties = { Setting.Property.NodeScope };
            final Setting.Property[] fallbackProperties = { Setting.Property.NodeScope, Setting.Property.Deprecated };
            final Setting<Integer> fallbackSizeSetting = sizeSetting(settings, fallbackName, size, fallbackPrefix, fallbackProperties);
            this.sizeSetting =
                    new Setting<>(
                            new Setting.SimpleKey(sizeKey),
                            fallbackSizeSetting,
                            s -> Setting.parseInt(s, 1, applyHardSizeLimit(settings, name), sizeKey),
                            properties);
            this.fallbackSizeSetting = fallbackSizeSetting;
            final Setting<Integer> fallbackQueueSizeSetting = queueSizeSetting(fallbackPrefix, queueSize, fallbackProperties);
            this.queueSizeSetting =
                    new Setting<>(
                            new Setting.SimpleKey(queueSizeKey),
                            fallbackQueueSizeSetting,
                            s -> Setting.parseInt(s, Integer.MIN_VALUE, queueSizeKey),
                            properties);
            this.fallbackQueueSizeSetting = fallbackQueueSizeSetting;
        }
    }

    private Setting<Integer> sizeSetting(
            final Settings settings, final String name, final int size, final String prefix, final Setting.Property[] properties) {
        final String sizeKey = settingsKey(prefix, "size");
        return new Setting<>(
                sizeKey,
                s -> Integer.toString(size),
                s -> Setting.parseInt(s, 1, applyHardSizeLimit(settings, name), sizeKey),
                properties);
    }

    private Setting<Integer> queueSizeSetting(final String prefix, final int queueSize, final Setting.Property[] properties) {
        return Setting.intSetting(settingsKey(prefix, "queue_size"), queueSize, properties);
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        if (fallbackSizeSetting == null && fallbackQueueSizeSetting == null) {
            return Arrays.asList(sizeSetting, queueSizeSetting);
        } else {
            assert fallbackSizeSetting != null && fallbackQueueSizeSetting != null;
            return Arrays.asList(sizeSetting, fallbackSizeSetting, queueSizeSetting, fallbackQueueSizeSetting);
        }
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
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName(settings.nodeName, name()));
        final ExecutorService executor =
                EsExecutors.newFixed(settings.nodeName + "/" + name(), size, queueSize, threadFactory, threadContext);
        final String name;
        if ("write".equals(name()) && Booleans.parseBoolean(System.getProperty("es.thread_pool.write.use_bulk_as_display_name", "false"))) {
            name = "bulk";
        } else {
            name = name();
        }
        final ThreadPool.Info info =
            new ThreadPool.Info(name, ThreadPool.ThreadPoolType.FIXED, size, size, null, queueSize < 0 ? null : new SizeValue(queueSize));
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], size [%d], queue size [%s]",
            info.getName(),
            info.getMax(),
            info.getQueueSize() == null ? "unbounded" : info.getQueueSize());
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
