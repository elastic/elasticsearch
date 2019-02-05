/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.AfterClass;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class UpgradeClusterClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    /**
     * Waits for the Machine Learning templates to be created by {@link org.elasticsearch.plugins.MetaDataUpgrader}
     */
    @Before
    public void waitForTemplates() throws Exception {
        List<String> templatesToWaitFor = XPackRestTestHelper.ML_POST_V660_TEMPLATES;

        // If upgrading from a version prior to v6.6.0 the set of templates
        // to wait for is different
        if (System.getProperty("tests.rest.suite").equals("old_cluster")) {
            String versionProperty = System.getProperty("tests.upgrade_from_version");
            if (versionProperty == null) {
                throw new IllegalStateException("System property 'tests.upgrade_from_version' not set, cannot start tests");
            }

            Version upgradeFromVersion = Version.fromString(versionProperty);
            if (upgradeFromVersion.before(Version.V_6_6_0)) {
                templatesToWaitFor = XPackRestTestHelper.ML_PRE_V660_TEMPLATES;
            }
        }

        XPackRestTestHelper.waitForTemplates(client(), templatesToWaitFor);
    }

    @AfterClass
    public static void upgradeSecurityIfNecessary() throws Exception {
        if (System.getProperty("tests.rest.suite").equals("old_cluster")) {
            Response response = client().performRequest("GET", "_nodes");
            ObjectPath objectPath = ObjectPath.createFromResponse(response);
            Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
            Version oldVersion = Version.fromString(objectPath.evaluate("nodes." + nodesAsMap.keySet().iterator().next() + ".version"));
            if (oldVersion.major < Version.CURRENT.major) {
                client().performRequest("POST", "/_xpack/migration/upgrade/.security");
            }
        }
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    public UpgradeClusterClientYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString(("test_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                // we increase the timeout here to 90 seconds to handle long waits for a green
                // cluster health. the waits for green need to be longer than a minute to
                // account for delayed shards
                .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
                .build();
    }
}
