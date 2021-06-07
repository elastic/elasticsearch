/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class AutoscalingRestIT extends ESClientYamlSuiteTestCase {

    public AutoscalingRestIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected Settings restAdminSettings() {
        final String value = basicAuthHeaderValue("autoscaling-admin", new SecureString("autoscaling-admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
    }

    @Override
    protected Settings restClientSettings() {
        final String value = basicAuthHeaderValue("autoscaling-user", new SecureString("autoscaling-user-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
    }

}
