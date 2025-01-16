/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.test.cluster.util.Version;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.test.cluster.util.Version.CURRENT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test suite for Lucene indices backward compatibility with N-2 versions during rolling upgrades. The test suite creates a cluster in N-2
 * version, then upgrades each node sequentially to N-1 version and finally upgrades each node sequentially to the current version. Test
 * methods are executed after each node upgrade.
 */
@TestCaseOrdering(RollingUpgradeIndexCompatibilityTestCase.TestCaseOrdering.class)
public abstract class RollingUpgradeIndexCompatibilityTestCase extends AbstractIndexCompatibilityTestCase {

    private final List<Version> nodesVersions;

    public RollingUpgradeIndexCompatibilityTestCase(@Name("cluster") List<Version> nodesVersions) {
        this.nodesVersions = nodesVersions;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Stream.of(
            // Begin on N-2
            List.of(VERSION_MINUS_2, VERSION_MINUS_2, VERSION_MINUS_2),
            // Rolling upgrade to VERSION_MINUS_1
            List.of(VERSION_MINUS_1, VERSION_MINUS_2, VERSION_MINUS_2),
            List.of(VERSION_MINUS_1, VERSION_MINUS_1, VERSION_MINUS_2),
            List.of(VERSION_MINUS_1, VERSION_MINUS_1, VERSION_MINUS_1),
            // Rolling upgrade to CURRENT
            List.of(CURRENT, VERSION_MINUS_1, VERSION_MINUS_1),
            List.of(CURRENT, CURRENT, VERSION_MINUS_1),
            List.of(CURRENT, CURRENT, CURRENT)
        ).map(nodesVersion -> new Object[] { nodesVersion }).toList();
    }

    @Override
    protected void maybeUpgrade() throws Exception {
        assertThat(nodesVersions, hasSize(NODES));

        for (int i = 0; i < NODES; i++) {
            var nodeName = cluster().getName(i);

            var expectedNodeVersion = nodesVersions.get(i);
            assertThat(expectedNodeVersion, notNullValue());

            var currentNodeVersion = nodesVersions().get(nodeName);
            assertThat(currentNodeVersion, notNullValue());
            assertThat(currentNodeVersion.onOrBefore(expectedNodeVersion), equalTo(true));

            if (currentNodeVersion.equals(expectedNodeVersion) == false) {
                closeClients();
                cluster().upgradeNodeToVersion(i, expectedNodeVersion);
                initClient();

                ensureHealth((request -> {
                    request.addParameter("timeout", "70s");
                    request.addParameter("wait_for_nodes", String.valueOf(NODES));
                    request.addParameter("wait_for_status", "yellow");
                }));
            }

            currentNodeVersion = nodesVersions().get(nodeName);
            assertThat(currentNodeVersion, equalTo(expectedNodeVersion));
        }
    }

    /**
     * Execute the test suite with the parameters provided by the {@link #parameters()} in nodes versions order.
     */
    public static class TestCaseOrdering implements Comparator<TestMethodAndParams> {
        @Override
        public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
            List<?> nodesVersions1 = asInstanceOf(List.class, o1.getInstanceArguments().get(0));
            assertThat(nodesVersions1, hasSize(NODES));
            List<?> nodesVersions2 = asInstanceOf(List.class, o2.getInstanceArguments().get(0));
            assertThat(nodesVersions2, hasSize(NODES));
            for (int i = 0; i < NODES; i++) {
                var nodeVersion1 = asInstanceOf(Version.class, nodesVersions1.get(i));
                var nodeVersion2 = asInstanceOf(Version.class, nodesVersions2.get(i));
                var result = nodeVersion1.compareTo(nodeVersion2);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }
}
