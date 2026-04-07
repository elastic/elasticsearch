/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.Before;

@TestCaseOrdering(YamlFileOrder.class)
public abstract class LicenseYamlSuiteTestCase extends ESClientYamlSuiteTestCase {

    public LicenseYamlSuiteTestCase(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    private static Boolean usingFulfillingCluster;

    @Before
    public void reinitializeClientsForCurrentCluster() throws Exception {
        if (usingFulfillingCluster == null || usingFulfillingCluster != isUsingFulfillingCluster()) {
            usingFulfillingCluster = isUsingFulfillingCluster();
            resetClients();
        }
    }

    private void resetClients() throws Exception {
        closeClient();
        closeClients();
        initClient();
        initAndResetContext();
    }

    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(Clusters.TEST_USER, new SecureString(Clusters.TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private boolean isUsingFulfillingCluster() {
        return getTestCandidate().getTestPath().contains("fulfilling");
    }

    protected abstract ElasticsearchCluster fulfillingCluster();

    protected abstract ElasticsearchCluster queryingCluster();

    @Override
    protected String getTestRestCluster() {
        return isUsingFulfillingCluster() ? fulfillingCluster().getHttpAddresses() : queryingCluster().getHttpAddresses();
    }

}
