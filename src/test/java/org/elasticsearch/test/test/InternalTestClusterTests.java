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

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SettingsSource;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.InternalTestCluster.clusterName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

/**
 * Basic test that ensure that the internal cluster reproduces the same
 * configuration given the same seed / input.
 */
public class InternalTestClusterTests extends ElasticsearchTestCase {

    public void testInitializiationIsConsistent() {
        long clusterSeed = randomLong();
        int minNumDataNodes = randomIntBetween(0, 9);
        int maxNumDataNodes = randomIntBetween(minNumDataNodes, 10);
        String clusterName = randomRealisticUnicodeOfCodepointLengthBetween(1, 10);
        SettingsSource settingsSource = SettingsSource.EMPTY;
        int numClientNodes = randomIntBetween(0, 10);
        boolean enableRandomBenchNodes = randomBoolean();
        int jvmOrdinal = randomIntBetween(0, 10);
        String nodePrefix = randomRealisticUnicodeOfCodepointLengthBetween(1, 10);

        InternalTestCluster cluster0 = new InternalTestCluster(clusterSeed, minNumDataNodes, maxNumDataNodes, clusterName, settingsSource, numClientNodes, enableRandomBenchNodes, jvmOrdinal, nodePrefix);
        InternalTestCluster cluster1 = new InternalTestCluster(clusterSeed, minNumDataNodes, maxNumDataNodes, clusterName, settingsSource, numClientNodes, enableRandomBenchNodes, jvmOrdinal, nodePrefix);
        assertClusters(cluster0, cluster1, true);

    }

    public static void assertClusters(InternalTestCluster cluster0, InternalTestCluster cluster1, boolean assertClusterName) {
        Settings defaultSettings0 = cluster0.getDefaultSettings();
        Settings defaultSettings1 = cluster1.getDefaultSettings();
        assertSettings(defaultSettings0, defaultSettings1, assertClusterName);
        assertThat(cluster0.numDataNodes(), equalTo(cluster1.numDataNodes()));
        assertThat(cluster0.numBenchNodes(), equalTo(cluster1.numBenchNodes()));
        if (assertClusterName) {
            assertThat(cluster0.getClusterName(), equalTo(cluster1.getClusterName()));
        }
    }

    public static void assertSettings(Settings left, Settings right, boolean compareClusterName) {
        ImmutableSet<Map.Entry<String, String>> entries0 = left.getAsMap().entrySet();
        Map<String, String> entries1 = right.getAsMap();
        assertThat(entries0.size(), equalTo(entries1.size()));
        for (Map.Entry<String, String> entry : entries0) {
            if(entry.getKey().equals(ClusterName.SETTING) && compareClusterName == false) {
                continue;
            }
            assertThat(entries1, hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    public void testBeforeTest() throws IOException {
        long clusterSeed = randomLong();
        int minNumDataNodes = randomIntBetween(0, 3);
        int maxNumDataNodes = randomIntBetween(minNumDataNodes, 4);
        final String clusterName = clusterName("shared", Integer.toString(CHILD_JVM_ID), clusterSeed);
        String clusterName1 = clusterName("shared", Integer.toString(CHILD_JVM_ID), clusterSeed);
        while (clusterName.equals(clusterName1)) {
            clusterName1 = clusterName("shared", Integer.toString(CHILD_JVM_ID), clusterSeed);   // spin until the time changes
        }
        SettingsSource settingsSource = SettingsSource.EMPTY;
        int numClientNodes = randomIntBetween(0, 2);
        boolean enableRandomBenchNodes = randomBoolean();
        int jvmOrdinal = randomIntBetween(0, 10);
        String nodePrefix = "foobar";

        InternalTestCluster cluster0 = new InternalTestCluster(clusterSeed, minNumDataNodes, maxNumDataNodes, clusterName, settingsSource, numClientNodes, enableRandomBenchNodes, jvmOrdinal, nodePrefix);
        InternalTestCluster cluster1 = new InternalTestCluster(clusterSeed, minNumDataNodes, maxNumDataNodes, clusterName1, settingsSource, numClientNodes, enableRandomBenchNodes, jvmOrdinal, nodePrefix);

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
            Iterator<Client> iterator1 = cluster1.iterator();
            for (Client client : cluster0) {
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
