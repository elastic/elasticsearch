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
import org.elasticsearch.client.LazyRefreshRestClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.AfterClass;
import org.junit.ClassRule;

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

    /**
     * Whether the cluster currently has state from a prior test that we have <em>deferred</em>
     * cleaning up. When true, the next test skips its YAML setup (the state is already there)
     * and the YAML teardown / framework wipe stay deferred. When the body of any test issues a
     * non-read HTTP request, that test's end-of-test cleanup runs and clears this flag, so the
     * test after it runs setup against a fresh cluster.
     */
    private static boolean deferredCleanupPending = false;

    /** Cached per-test result for {@link #preserveClusterUponCompletion()} so its decision and
     *  side effects run exactly once even though the framework calls it twice
     *  ({@code cleanUpCluster} and {@code assertEmptyProjects}). */
    private Boolean cachedPreserve = null;

    public XpackWithMultipleProjectsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        deferredCleanupPending = false;
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @AfterClass
    public static void resetDeferredCleanupState() {
        deferredCleanupPending = false;
    }

    @Override
    protected boolean skipSetupSections() {
        return deferredCleanupPending;
    }

    @Override
    protected boolean skipTeardownSections() {
        return LazyRefreshRestClient.writeOccurred() == false;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        if (cachedPreserve != null) {
            return cachedPreserve;
        }
        boolean writeHappened = LazyRefreshRestClient.writeOccurred();
        cachedPreserve = (writeHappened == false);
        deferredCleanupPending = cachedPreserve;
        return cachedPreserve;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
