/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.test.rest.RestTestCandidate;

import java.io.IOException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;

public class WatcherWithShieldInsufficientRoleIT extends WatcherWithShieldIT {
    public WatcherWithShieldInsufficientRoleIT(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    public void test() throws IOException {
        try {
            super.test();
            fail();
        } catch(AssertionError ae) {
            assertThat(ae.getMessage(), anyOf(containsString("action [cluster:monitor/watcher/"), containsString("action [cluster:admin/watcher/")));
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [powerless_user]"));
        }
    }

    @Override
    protected String[] getCredentials() {
        return new String[]{"powerless_user", "changeme"};
    }
}
