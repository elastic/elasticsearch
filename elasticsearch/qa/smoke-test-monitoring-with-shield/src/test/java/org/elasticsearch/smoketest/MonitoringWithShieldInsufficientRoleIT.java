/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.test.rest.RestTestCandidate;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class MonitoringWithShieldInsufficientRoleIT extends MonitoringWithShieldIT {

    public MonitoringWithShieldInsufficientRoleIT(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    public void test() throws IOException {
        try {
            super.test();
            fail("should have failed because of missing role");
        } catch(AssertionError ae) {
            assertThat(ae.getMessage(), containsString("action [cluster:admin/xpack/monitoring/bulk]"));
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [no_monitored_system]"));
        }
    }

    @Override
    protected String[] getCredentials() {
        return new String[]{"no_monitored_system", "changeme"};
    }
}
