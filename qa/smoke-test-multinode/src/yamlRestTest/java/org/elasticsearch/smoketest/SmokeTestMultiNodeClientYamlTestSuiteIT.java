/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

@TimeoutSuite(millis = 40 * TimeUnits.MINUTE) // some of the windows test VMs are slow as hell
public class SmokeTestMultiNodeClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .module("mapper-extras")
        .module("ingest-common")
        .module("reindex")
        // The first node does not have the ingest role so we're sure ingest requests are forwarded:
        .node(0, n -> n.setting("node.roles", "[master,data,ml,remote_cluster_client,transform]"))
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .feature(FeatureFlag.DATA_STREAM_LIFECYCLE_ENABLED)
        .feature(FeatureFlag.SYNONYMS_ENABLED)
        .build();

    public SmokeTestMultiNodeClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
