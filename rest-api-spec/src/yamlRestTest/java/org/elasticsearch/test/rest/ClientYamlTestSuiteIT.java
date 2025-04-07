/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

/** Rest integration test. Runs against a cluster started by {@code gradle integTest} */

// The default 20 minutes timeout isn't always enough, but Darwin CI hosts are incredibly slow...
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class ClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("mapper-extras")
        .module("rest-root")
        .module("reindex")
        .module("analysis-common")
        .module("health-shards-availability")
        .module("data-streams")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .feature(FeatureFlag.SUB_OBJECTS_AUTO_ENABLED)
        .feature(FeatureFlag.DOC_VALUES_SKIPPER)
        .feature(FeatureFlag.USE_LUCENE101_POSTINGS_FORMAT)
        .build();

    public ClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
