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
package org.elasticsearch.test.test;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.transport.TransportSettings;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

/**
 * Basic test that ensure that the internal cluster reproduces the same
 * configuration given the same seed / input.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // doesn't work with potential multi data path from test cluster yet
public class InternalTestClusterTests extends ESTestCase {

    public void testInitializiationIsConsistent() {
        long clusterSeed = randomLong();
        boolean masterNodes = randomBoolean();
        int minNumDataNodes = randomIntBetween(0, 9);
        int maxNumDataNodes = randomIntBetween(minNumDataNodes, 10);
        String clusterName = randomRealisticUnicodeOfCodepointLengthBetween(1, 10);
        NodeConfigurationSource nodeConfigurationSource = NodeConfigurationSource.EMPTY;
        int numClientNodes = randomIntBetween(0, 10);
        boolean enableHttpPipelining = randomBoolean();
        String nodePrefix = randomRealisticUnicodeOfCodepointLengthBetween(1, 10);

        Path baseDir = createTempDir();
        InternalTestCluster cluster0 = new InternalTestCluster("local", clusterSeed, baseDir, masterNodes,
            minNumDataNodes, maxNumDataNodes, clusterName, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());
        InternalTestCluster cluster1 = new InternalTestCluster("local", clusterSeed, baseDir, masterNodes,
            minNumDataNodes, maxNumDataNodes, clusterName, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());
        // TODO: this is not ideal - we should have a way to make sure ports are initialized in the same way
        assertClusters(cluster0, cluster1, false);

    }

    /**
     * a set of settings that are expected to have different values betweem clusters, even they have been initialized with the same
     * base settins.
     */
    final static Set<String> clusterUniqueSettings = new HashSet<>();

    static {
        clusterUniqueSettings.add(ClusterName.CLUSTER_NAME_SETTING.getKey());
        clusterUniqueSettings.add(TransportSettings.PORT.getKey());
        clusterUniqueSettings.add("http.port");
    }

    public static void assertClusters(InternalTestCluster cluster0, InternalTestCluster cluster1, boolean checkClusterUniqueSettings) {
        Settings defaultSettings0 = cluster0.getDefaultSettings();
        Settings defaultSettings1 = cluster1.getDefaultSettings();
        assertSettings(defaultSettings0, defaultSettings1, checkClusterUniqueSettings);
        assertThat(cluster0.numDataNodes(), equalTo(cluster1.numDataNodes()));
        if (checkClusterUniqueSettings) {
            assertThat(cluster0.getClusterName(), equalTo(cluster1.getClusterName()));
        }
    }

    public static void assertSettings(Settings left, Settings right, boolean checkClusterUniqueSettings) {
        Set<Map.Entry<String, String>> entries0 = left.getAsMap().entrySet();
        Map<String, String> entries1 = right.getAsMap();
        assertThat(entries0.size(), equalTo(entries1.size()));
        for (Map.Entry<String, String> entry : entries0) {
            if (clusterUniqueSettings.contains(entry.getKey()) && checkClusterUniqueSettings == false) {
                continue;
            }
            assertThat(entries1, hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    public void testBeforeTest() throws Exception {
        long clusterSeed = randomLong();
        boolean masterNodes = randomBoolean();
        int minNumDataNodes = randomIntBetween(0, 3);
        int maxNumDataNodes = randomIntBetween(minNumDataNodes, 4);
        final String clusterName1 = "shared1";//clusterName("shared1", clusterSeed);
        final String clusterName2 = "shared2";//clusterName("shared", Integer.toString(CHILD_JVM_ID), clusterSeed);
        /*while (clusterName.equals(clusterName1)) {
            clusterName1 = clusterName("shared", Integer.toString(CHILD_JVM_ID), clusterSeed);   // spin until the time changes
        }*/
        NodeConfigurationSource nodeConfigurationSource = NodeConfigurationSource.EMPTY;
        int numClientNodes = randomIntBetween(0, 2);
        boolean enableHttpPipelining = randomBoolean();
        int jvmOrdinal = randomIntBetween(0, 10);
        String nodePrefix = "foobar";

        Path baseDir = createTempDir();
        InternalTestCluster cluster0 = new InternalTestCluster("local", clusterSeed, baseDir, masterNodes,
            minNumDataNodes, maxNumDataNodes, clusterName1, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());
        InternalTestCluster cluster1 = new InternalTestCluster("local", clusterSeed, baseDir, masterNodes,
            minNumDataNodes, maxNumDataNodes, clusterName2, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());

        assertClusters(cluster0, cluster1, false);
        long seed = randomLong();
        try {
            {
                Random random = new Random(seed);
                cluster0.beforeTest(random, random.nextDouble());
            }
            {
                Random random = new Random(seed);
                cluster1.beforeTest(random, random.nextDouble());
            }
            assertArrayEquals(cluster0.getNodeNames(), cluster1.getNodeNames());
            Iterator<Client> iterator1 = cluster1.getClients().iterator();
            for (Client client : cluster0.getClients()) {
                assertTrue(iterator1.hasNext());
                Client other = iterator1.next();
                assertSettings(client.settings(), other.settings(), false);
            }
            assertArrayEquals(cluster0.getNodeNames(), cluster1.getNodeNames());
            cluster0.afterTest();
            cluster1.afterTest();
        } finally {
            IOUtils.close(cluster0, cluster1);
        }
    }
}
