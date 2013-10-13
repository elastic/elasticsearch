/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.node;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.Matchers.is;

public class InternalNodeTests extends ElasticsearchTestCase {

    @Test
    public void testDefaultPluginConfiguration() throws Exception {

        Settings settings = settingsBuilder()
                .put("plugin.types", TestPlugin.class.getName())
                .put("name", "test")
                .build();

        InternalNode node = (InternalNode) nodeBuilder()
                .settings(settings)
                .build();

        TestService service = node.injector().getInstance(TestService.class);
        assertThat(service.state.initialized(), is(true));
        node.start();
        assertThat(service.state.started(), is(true));
        node.stop();
        assertThat(service.state.stopped(), is(true));
        node.close();
        assertThat(service.state.closed(), is(true));
    }

    public static class TestPlugin extends AbstractPlugin {

        public TestPlugin() {
        }

        @Override
        public String name() {
            return "test";
        }

        @Override
        public String description() {
            return "test plugin";
        }

        @Override
        public Collection<Class<? extends LifecycleComponent>> services() {
            Collection<Class<? extends LifecycleComponent>> services = new ArrayList<Class<? extends LifecycleComponent>>();
            services.add(TestService.class);
            return services;
        }
    }

    @Singleton
    public static class TestService extends AbstractLifecycleComponent<TestService> {

        private Lifecycle state;

        @Inject
        public TestService(Settings settings) {
            super(settings);
            logger.info("initializing");
            state = new Lifecycle();
        }

        @Override
        protected void doStart() throws ElasticSearchException {
            logger.info("starting");
            state.moveToStarted();
        }

        @Override
        protected void doStop() throws ElasticSearchException {
            logger.info("stopping");
            state.moveToStopped();
        }

        @Override
        protected void doClose() throws ElasticSearchException {
            logger.info("closing");
            state.moveToClosed();
        }
    }

}