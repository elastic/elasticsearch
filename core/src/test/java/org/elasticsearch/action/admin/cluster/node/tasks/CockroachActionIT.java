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
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class CockroachActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TestCockroachActionPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .build();
    }

    public void testCockroachAction() throws Exception {
        // Randomly create an empty index to make sure the type is created automatically
        TestCockroachActionPlugin.TestCockroachAction.INSTANCE.newRequestBuilder(client()).testParam("Blah").get();
    }

    public void testCockroachFailureToStartAction() throws Exception {
        // Randomly create an empty index to make sure the type is created automatically

        assertThrows(
            TestCockroachActionPlugin.TestCockroachAction.INSTANCE.newRequestBuilder(client())
                .testParam("fail to start")
                .targetNode("nowhere"),
            IllegalStateException.class,
            "No nodes available to execute the cockroach action");

    }

}
