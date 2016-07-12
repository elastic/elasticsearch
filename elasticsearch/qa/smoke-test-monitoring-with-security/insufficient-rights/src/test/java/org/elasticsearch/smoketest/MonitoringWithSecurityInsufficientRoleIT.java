/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;

import java.io.IOException;

import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;

public class MonitoringWithSecurityInsufficientRoleIT extends ESRestTestCase {

    public MonitoringWithSecurityInsufficientRoleIT(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ESRestTestCase.createParameters(0, 1);
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("not_monitoring_system", new SecuredString("changeme".toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecuredString("changeme".toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    @Override
    public void test() throws Exception {
        try {
            super.test();
            fail("should have failed because of missing role");
        } catch(AssertionError ae) {
            assertThat(ae.getMessage(), containsString("action [cluster:admin/xpack/monitoring/bulk]"));
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [not_monitoring_system]"));
        }
    }

}
