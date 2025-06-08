/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.multiproject.test;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
public class CoreWithMultipleProjectsClientYamlTestSuiteIT extends MultipleProjectsClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("constant-keyword")
        .module("mapper-extras")
        .module("rank-eval")
        .module("x-pack-ilm")
        .module("x-pack-stack")
        .module("ingest-common")
        .module("reindex")
        .module("wildcard")
        .module("analysis-common")
        .module("health-shards-availability")
        .module("data-streams")
        .module("test-multi-project")
        .module("lang-mustache")
        .module("parent-join")
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .user(USER, PASS)
        .systemProperty("es.queryable_built_in_roles_enabled", "false")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .nodes(2)
        .build();

    public CoreWithMultipleProjectsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
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
