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
import java.util.stream.Stream;

import static org.elasticsearch.test.cluster.util.Version.CURRENT;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test suite for Lucene indices backward compatibility with N-2 versions after full cluster restart upgrades. The test suite creates a
 * cluster in N-2 version, then upgrades it to N-1 version and finally upgrades it to the current version. Test methods are executed after
 * each upgrade.
 */
@TestCaseOrdering(FullClusterRestartIndexCompatibilityTestCase.TestCaseOrdering.class)
public abstract class FullClusterRestartIndexCompatibilityTestCase extends AbstractIndexCompatibilityTestCase {

    private final Version clusterVersion;

    public FullClusterRestartIndexCompatibilityTestCase(@Name("cluster") Version clusterVersion) {
        this.clusterVersion = clusterVersion;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Stream.of(VERSION_MINUS_2, VERSION_MINUS_1, CURRENT).map(v -> new Object[] { v }).toList();
    }

    @Override
    protected void maybeUpgrade() throws Exception {
        if (nodesVersions().values().stream().anyMatch(version -> version.before(clusterVersion))) {
            cluster().upgradeToVersion(clusterVersion);
            closeClients();
            initClient();
        }
        assertThat(isFullyUpgradedTo(clusterVersion), equalTo(true));
    }

    /**
     * Execute the test suite with the parameters provided by the {@link #parameters()} in version order.
     */
    public static class TestCaseOrdering implements Comparator<TestMethodAndParams> {
        @Override
        public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
            var version1 = (Version) o1.getInstanceArguments().get(0);
            var version2 = (Version) o2.getInstanceArguments().get(0);
            return version1.compareTo(version2);
        }
    }
}
