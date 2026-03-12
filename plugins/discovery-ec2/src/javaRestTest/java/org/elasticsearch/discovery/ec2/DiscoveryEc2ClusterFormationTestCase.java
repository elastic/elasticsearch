/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

public abstract class DiscoveryEc2ClusterFormationTestCase extends ESRestTestCase {

    protected abstract ElasticsearchCluster getCluster();

    @Override
    protected String getTestRestCluster() {
        return getCluster().getHttpAddresses();
    }

    public void testClusterFormation() throws IOException {
        final var cluster = getCluster();
        final var expectedAddresses = new HashSet<>(cluster.getAvailableTransportEndpoints());
        final var addressesPattern = Pattern.compile(".* using dynamic transport addresses \\[(.*)]");

        assertThat(cluster.getNumNodes(), greaterThan(1)); // multiple node cluster means discovery must have worked
        for (int nodeIndex = 0; nodeIndex < cluster.getNumNodes(); nodeIndex++) {
            try (
                var logStream = cluster.getNodeLog(nodeIndex, LogType.SERVER);
                var logReader = new InputStreamReader(logStream, StandardCharsets.UTF_8);
                var bufReader = new BufferedReader(logReader)
            ) {
                do {
                    final var line = bufReader.readLine();
                    if (line == null) {
                        break;
                    }

                    final var matcher = addressesPattern.matcher(line);
                    if (matcher.matches()) {
                        for (final var address : matcher.group(1).split(", ")) {
                            // TODO also add some nodes to the DescribeInstances output which are filtered out, and verify that we do not
                            // see their addresses here
                            assertThat(expectedAddresses, hasItem(address));
                        }
                    }
                } while (true);
            }
        }
    }

    protected static String getIdentifierPrefix(String testSuiteName) {
        return testSuiteName + "-" + Integer.toString(Murmur3HashFunction.hash(testSuiteName + System.getProperty("tests.seed")), 16) + "-";
    }
}
