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

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

/**
 */
public class CustomThreadPoolTests extends ElasticsearchSingleNodeTest {

    public void testAddAndRemoveCustomThreadPool() {
        Executor executor = EsExecutors.newFixed(2, 10, EsExecutors.daemonThreadFactory("my-name"));
        ThreadPool.Info info = new ThreadPool.Info("my-name", "fixed");

        ThreadPool threadPool = ((InternalNode) node()).injector().getInstance(ThreadPool.class);
        threadPool.addCustomExecutor(executor, info);

        boolean found = false;
        NodesInfoResponse infoResponse = client().admin().cluster().prepareNodesInfo().get();
        for (ThreadPool.Info tInfo : infoResponse.getNodes()[0].getThreadPool()) {
            if ("my-name".equals(tInfo.getName())) {
                found = true;
                assertThat(tInfo.getType(), equalTo("fixed"));
            }
        }
        assertThat(found, is(true));

        found = false;
        NodesStatsResponse statsResponse = client().admin().cluster().prepareNodesStats().setThreadPool(true).get();
        for (ThreadPoolStats.Stats tStats : statsResponse.getNodes()[0].getThreadPool()) {
            if (tStats.getName().equals("my-name")) {
                found = true;
            }
        }
        assertThat(found, is(true));

        threadPool.removeCustomExecutor("my-name");
        infoResponse = client().admin().cluster().prepareNodesInfo().get();
        found = false;
        for (ThreadPool.Info tInfo : infoResponse.getNodes()[0].getThreadPool()) {
            if ("my-name".equals(tInfo.getName())) {
                found = true;
            }
        }
        assertThat(found, is(false));

        found = false;
        statsResponse = client().admin().cluster().prepareNodesStats().setThreadPool(true).get();
        for (ThreadPoolStats.Stats tStats : statsResponse.getNodes()[0].getThreadPool()) {
            if (tStats.getName().equals("my-name")) {
                found = true;
            }
        }
        assertThat(found, is(false));
    }

}
