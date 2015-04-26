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

package org.elasticsearch.stresstest.leaks;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.network.NetworkService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.process.ProcessService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class GenericStatsLeak {

    public static void main(String[] args) {
        Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder()
                .put("monitor.os.refresh_interval", 0)
                .put("monitor.process.refresh_interval", 0)
                .put("monitor.network.refresh_interval", 0)
        ).node();

        JvmService jvmService = node.injector().getInstance(JvmService.class);
        OsService osService = node.injector().getInstance(OsService.class);
        ProcessService processService = node.injector().getInstance(ProcessService.class);
        NetworkService networkService = node.injector().getInstance(NetworkService.class);

        while (true) {
            jvmService.stats();
            osService.stats();
            processService.stats();
            networkService.stats();
        }
    }
}