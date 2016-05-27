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

import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class ExecutorBuilder<U extends ExecutorBuilder.ExecutorSettings> {

    private final String name;

    public ExecutorBuilder(String name) {
        this.name = name;
    }

    protected String name() {
        return name;
    }

    protected static String settingsKey(final String prefix, final String name, final String key) {
        return String.join(prefix, name, key);
    }

    public abstract List<Setting<?>> registerSettings();

    public void registerListener(ThreadPool threadPool, ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdater(new AbstractScopedSettings.SettingUpdater<U>() {
            @Override
            public boolean hasChanged(Settings current, Settings previous) {
                for (final Setting<?> setting : registerSettings()) {
                    if (!Objects.equals(setting.get(previous), setting.get(current))) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public U getValue(Settings current, Settings previous) {
                return settings(current);
            }

            @Override
            public void apply(U value, Settings current, Settings previous) {
                final ThreadPool.ExecutorHolder previousHolder = threadPool.holder(name);
                threadPool.registerExecutor(holder(previousHolder, value, threadPool.getThreadContext()));
            }
        });
    }

    public abstract U settings(Settings settings);

    public abstract ThreadPool.ExecutorHolder holder(ThreadPool.ExecutorHolder previousHolder, U settings, ThreadContext threadContext);

    public abstract String formatInfo(ThreadPool.Info info);

    static abstract class ExecutorSettings {

        protected final String nodeName;

        public ExecutorSettings(String nodeName) {
            this.nodeName = nodeName;
        }

    }

}
