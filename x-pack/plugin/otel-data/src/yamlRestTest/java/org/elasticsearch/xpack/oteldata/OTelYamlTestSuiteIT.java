/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

public class OTelYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("constant-keyword")
        .module("counted-keyword")
        .module("data-streams")
        .module("ingest-common")
        .module("ingest-geoip")
        .module("ingest-user-agent")
        .module("lang-mustache")
        .module("mapper-extras")
        .module("wildcard")
        .module("x-pack-analytics")
        .module("x-pack-otel-data")
        .module("x-pack-aggregate-metric")
        .module("x-pack-ilm")
        .module("x-pack-stack")
        .module("mapper-version")
        .setting("ingest.geoip.downloader.enabled", "false")
        .build();

    public OTelYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
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
