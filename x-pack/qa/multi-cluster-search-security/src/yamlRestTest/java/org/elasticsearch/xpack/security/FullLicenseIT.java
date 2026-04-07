/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@TestCaseOrdering(YamlFileOrder.class)
public class FullLicenseIT extends LicenseYamlSuiteTestCase {

    private static final ElasticsearchCluster fulfillingCluster = Clusters.fulfillingCluster("trial");
    private static final ElasticsearchCluster queryingCluster = Clusters.queryingCluster(
        "trial",
        ESTestCase::randomBoolean,
        () -> fulfillingCluster.getTransportEndpoint(0)
    );

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryingCluster);

    public FullLicenseIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters("full_license");
    }

    @Override
    protected ElasticsearchCluster fulfillingCluster() {
        return fulfillingCluster;
    }

    @Override
    protected ElasticsearchCluster queryingCluster() {
        return queryingCluster;
    }

}
