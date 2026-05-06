/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;

public class WatcherPagerDutyYamlTestSuiteIT extends AbstractWatcherThirdPartyYamlTestSuiteIT {

    @ClassRule
    public static ElasticsearchCluster cluster = baseClusterBuilder().keystore(
        "xpack.notification.pagerduty.account.test_account.secure_service_api_key",
        System.getenv("pagerduty_service_api_key")
    ).build();

    @Override
    protected ElasticsearchCluster getCluster() {
        return cluster;
    }

    public WatcherPagerDutyYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }
}
