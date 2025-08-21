/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

public class CcrRestIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        // TODO: Switch to ingeg-test when we fix the xpack info api
        .distribution(DistributionType.DEFAULT)
        .module("x-pack-ccr")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .user("ccr-user", "ccr-user-password", "superuser", false)
        // Disable assertions in FollowingEngineAssertions, otherwise an AssertionError is thrown before
        // indexing a document directly in a follower index. In a rest test we like to test the exception
        // that is thrown in production when indexing a document directly in a follower index.
        .jvmArg("-da:org.elasticsearch.xpack.ccr.index.engine.FollowingEngineAssertions")
        .build();

    public CcrRestIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        final String ccrUserAuthHeaderValue = basicAuthHeaderValue("ccr-user", new SecureString("ccr-user-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", ccrUserAuthHeaderValue).build();
    }

    @Before
    public void waitForRequirements() throws Exception {
        waitForActiveLicense(adminClient());
    }

    @After
    public void cleanup() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith("indices:data/read/xpack/ccr/shard_changes"));
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
