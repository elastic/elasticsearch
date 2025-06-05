/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.multiproject.test;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

import java.util.Objects;

/**
 * Base class for running YAML Rest tests against a cluster with multiple projects
 */
public abstract class MultipleProjectsClientYamlSuiteTestCase extends ESClientYamlSuiteTestCase {

    /**
     * Username to use to execute tests
     */
    protected static final String USER = Objects.requireNonNull(System.getProperty("tests.rest.cluster.username", "test_admin"));
    /**
     * Password for {@link #USER}.
     */
    protected static final String PASS = Objects.requireNonNull(System.getProperty("tests.rest.cluster.password", "test-password"));

    public MultipleProjectsClientYamlSuiteTestCase(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
