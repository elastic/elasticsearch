/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class CcrRestIT extends ESClientYamlSuiteTestCase {

    public CcrRestIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        final String ccrUserAuthHeaderValue = basicAuthHeaderValue("ccr-user", new SecureString("ccr-user-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", ccrUserAuthHeaderValue).build();
    }

    @Before
    public void waitForRequirements() throws Exception {
        ESRestTestCase.waitForActiveLicense(adminClient());
    }

    @After
    public void cleanup() throws Exception {
        ESRestTestCase.waitForPendingTasks(adminClient(), taskName -> taskName.startsWith(ShardChangesAction.NAME));
    }

}
