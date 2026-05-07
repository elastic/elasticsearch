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
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@TimeoutSuite(millis = 60 * TimeUnits.MINUTE)
public class XpackWithMultipleProjectsClientYamlTestSuiteIT extends MultipleProjectsClientYamlSuiteTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("yamlRestTest")
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.ml.enabled", "true")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        // Integration tests are supposed to enable/disable exporters before/after each test
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "testnode.pem")
        .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        // disable ILM history, since it disturbs tests using _all
        .setting("indices.lifecycle.history_index_enabled", "false")
        .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .configFile("testnode.pem", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
        .configFile("service_tokens", Resource.fromClasspath("service_tokens"))
        .user(USER, PASS)
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .systemProperty("es.queryable_built_in_roles_enabled", () -> {
            final String enabled = System.getProperty("es.queryable_built_in_roles_enabled");
            return Objects.requireNonNullElse(enabled, "");
        })
        .build();

    /** Per-yaml-file count of tests not yet completed in this JVM. */
    private static final Map<String, Integer> remainingPerFile = new HashMap<>();

    /** Yaml file whose setup state is currently warm in the cluster, or null if cluster was wiped. */
    private static String warmFile = null;

    /** Cached result for {@link #preserveClusterUponCompletion()} — the framework calls it
     *  multiple times per test (cleanUpCluster, assertEmptyProjects), but our implementation
     *  has side effects (decrementing the per-file counter, updating {@link #warmFile}) that
     *  must run exactly once per test. */
    private Boolean cachedPreserve = null;

    public XpackWithMultipleProjectsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        Iterable<Object[]> base = ESClientYamlSuiteTestCase.createParameters();
        remainingPerFile.clear();
        for (Object[] p : base) {
            ClientYamlTestCandidate c = (ClientYamlTestCandidate) p[0];
            remainingPerFile.merge(c.getSuitePath(), 1, Integer::sum);
        }
        warmFile = null;
        return base;
    }

    @Override
    protected boolean skipSetupSections() {
        return getTestCandidate().getSuitePath().equals(warmFile);
    }

    @Override
    protected boolean skipTeardownSections() {
        Integer remaining = remainingPerFile.get(getTestCandidate().getSuitePath());
        return remaining != null && remaining > 1;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        if (cachedPreserve != null) {
            return cachedPreserve;
        }
        String currentFile = getTestCandidate().getSuitePath();
        Integer remaining = remainingPerFile.get(currentFile);
        boolean preserve = remaining != null && remaining > 1;
        if (preserve) {
            warmFile = currentFile;
        } else {
            warmFile = null;
        }
        if (remaining != null) {
            if (remaining <= 1) {
                remainingPerFile.remove(currentFile);
            } else {
                remainingPerFile.put(currentFile, remaining - 1);
            }
        }
        cachedPreserve = preserve;
        return preserve;
    }

    /**
     * On the first non-read request after a same-file setup share, wipe the cluster (clearing
     * any residue from previous tests' bodies) before the YAML setup re-runs against a clean state.
     */
    @Override
    protected void runDeferredCleanupAndSetup() {
        try {
            wipeCluster();
        } catch (Exception e) {
            throw new AssertionError("failed to wipe cluster before deferred setup", e);
        }
        warmFile = null;
        super.runDeferredCleanupAndSetup();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
