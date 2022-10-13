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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.List;

/**
 * Base class for executor builders.
 *
 * @param <U> the underlying type of the executor settings
 */
public abstract class ExecutorBuilder<U extends ExecutorBuilder.ExecutorSettings> {

    private final String name;

    public ExecutorBuilder(String name) {
        this.name = name;
    }

    protected String name() {
        return name;
    }

    protected static String settingsKey(final String prefix, final String key) {
        return String.join(".", prefix, key);
    }

    protected static int applyHardSizeLimit(final Settings settings, final String name) {
        if (name.equals("bulk")
            || name.equals(ThreadPool.Names.WRITE)
            || name.equals(ThreadPool.Names.SYSTEM_WRITE)
            || name.equals(ThreadPool.Names.SYSTEM_CRITICAL_WRITE)) {
            return 1 + EsExecutors.allocatedProcessors(settings);
        } else {
            return Integer.MAX_VALUE;
        }
    }

    /**
     * The list of settings this builder will register.
     *
     * @return the list of registered settings
     */
    public abstract List<Setting<?>> getRegisteredSettings();

    /**
     * Return an executor settings object from the node-level settings.
     *
     * @param settings the node-level settings
     * @return the executor settings object
     */
    abstract U getSettings(Settings settings);

    /**
     * Builds the executor with the specified executor settings.
     *
     * @param settings      the executor settings
     * @param threadContext the current thread context
     * @return a new executor built from the specified executor settings
     */
    abstract ThreadPool.ExecutorHolder build(U settings, ThreadContext threadContext);

    /**
     * Format the thread pool info object for this executor.
     *
     * @param info the thread pool info object to format
     * @return a formatted thread pool info (useful for logging)
     */
    abstract String formatInfo(ThreadPool.Info info);

    abstract static class ExecutorSettings {

        protected final String nodeName;

        ExecutorSettings(String nodeName) {
            this.nodeName = nodeName;
        }

    }

}
