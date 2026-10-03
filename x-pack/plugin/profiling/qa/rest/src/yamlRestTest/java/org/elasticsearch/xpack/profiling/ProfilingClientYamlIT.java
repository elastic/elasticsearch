/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

/**
 * Runs the Universal Profiling YAML REST tests against a dedicated single-node cluster.
 *
 * <p>These tests previously ran inside the monolithic {@code :x-pack:plugin:yamlRestTest}
 * ({@code XPackRestIT}) suite alongside every other x-pack feature. On slow CI workers the
 * shared suite could exhaust its per-suite timeout and abandon whichever test was in flight;
 * they were relocated here so that profiling runs in its own bounded suite. The YAML sources
 * still live under {@code x-pack/plugin/src/yamlRestTest/resources/rest-api-spec/test/profiling/}
 * and are pulled in via {@code restResources.restTests.includeXpack 'profiling'}.
 *
 * <p>A trial license is used because Universal Profiling is an {@code ENTERPRISE}-mode licensed
 * feature (see {@code ProfilingLicenseChecker}); the profiling APIs return a compliance error
 * under a basic license.
 */
public class ProfilingClientYamlIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .build();

    public ProfilingClientYamlIT(final ClientYamlTestCandidate testCandidate) {
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
