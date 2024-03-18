/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.Version;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase.Mode.ASYNC;

public class MixedClusterEsqlSpecIT extends EsqlSpecTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    static final Version bwcVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    public MixedClusterEsqlSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase, Mode mode) {
        super(fileName, groupName, testName, lineNumber, testCase, mode);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);
        assumeTrue("Test " + testName + " is skipped on " + bwcVersion, isEnabled(testName, bwcVersion));
        if (mode == ASYNC) {
            assumeTrue("Async is not supported on " + bwcVersion, supportsAsync());
        }
    }

    @Override
    protected boolean supportsAsync() {
        return bwcVersion.onOrAfter(Version.V_8_13_0);
    }
}
