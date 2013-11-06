/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.azure.itest;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Ignore;
import org.junit.Test;

public class AzureSimpleITest {

    @Test @Ignore
    public void one_node_should_run() {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                //.put("gateway.type", "local")
                .put("path.data", "./target/es/data")
                .put("path.logs", "./target/es/logs")
                .put("path.work", "./target/es/work");
        NodeBuilder.nodeBuilder().settings(builder).node();
    }
}
