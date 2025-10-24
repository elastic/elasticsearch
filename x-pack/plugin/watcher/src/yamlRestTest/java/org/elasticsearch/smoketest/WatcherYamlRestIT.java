/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;

/**
 * Runs the YAML rest tests against an external cluster
 */
public class WatcherYamlRestIT extends WatcherYamlSuiteTestCase {
    public WatcherYamlRestIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ClassRule
    public static ElasticsearchCluster cluster = watcherClusterSpec().build();

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters(new String[] { "mustache", "painless", "watcher" });
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
