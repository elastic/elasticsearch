/*
 * Licensed to ElasticSearch and Shay Banon under one
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

package org.elasticsearch.test.integration.indices.store;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleDistributorTests extends AbstractNodesTests {
    protected Environment environment;

    @BeforeClass
    public void getTestEnvironment() {
        environment = ((InternalNode) startNode("node0")).injector().getInstance(Environment.class);
        closeNode("node0");
    }

    @AfterClass
    public void closeNodes() {
        closeAllNodes();
    }

    public final static String[] STORE_TYPES = {"fs", "simplefs", "niofs", "mmapfs"};

    @Test
    public void testAvailableSpaceDetection() {
        File dataRoot = environment.dataFiles()[0];
        startNode("node1", settingsBuilder().putArray("path.data", new File(dataRoot, "data1").getAbsolutePath(), new File(dataRoot, "data2").getAbsolutePath()));
        for (String store : STORE_TYPES) {
            try {
                client("node1").admin().indices().prepareDelete("test").execute().actionGet();
            } catch (IndexMissingException ex) {
                // Ignore
            }
            client("node1").admin().indices().prepareCreate("test")
                    .setSettings(settingsBuilder()
                            .put("index.store.distributor", StrictDistributor.class.getCanonicalName())
                            .put("index.store.type", store)
                            .put("index.number_of_replicas", 0)
                            .put("index.number_of_shards", 1)
                    )
                    .execute().actionGet();
            assertThat(client("node1").admin().cluster().prepareHealth("test").setWaitForGreenStatus()
                    .setTimeout(TimeValue.timeValueSeconds(5)).execute().actionGet().isTimedOut(), equalTo(false));
        }
    }

}
