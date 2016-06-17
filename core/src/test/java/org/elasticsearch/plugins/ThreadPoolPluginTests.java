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

package org.elasticsearch.plugins;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Test that {@link ThreadPoolPlugin}s get the ThreadPool at an appropriate time. 
 */
public class ThreadPoolPluginTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    public void testPluginOk() {
        assertSame(getInstanceFromNode(ThreadPool.class), TestPlugin.threadPool);
    }

    public static class TestPlugin extends Plugin implements ThreadPoolPlugin {
        private static ThreadPool threadPool;

        @Override
        public void setThreadPool(ThreadPool threadPool) {
            TestPlugin.threadPool = threadPool;
        }

        @Override
        public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
            assertNull("setThreadPool should not have already been called", threadPool);
            return super.getExecutorBuilders(settings);
        }

        @Override
        public Collection<Module> nodeModules() {
            assertNotNull("setThreadPool should have already been called", threadPool);
            return super.nodeModules();
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Collection<Class<? extends LifecycleComponent>> nodeServices() {
            assertNotNull("setThreadPool should have already been called", threadPool);
            return super.nodeServices();
        }
    }
}
