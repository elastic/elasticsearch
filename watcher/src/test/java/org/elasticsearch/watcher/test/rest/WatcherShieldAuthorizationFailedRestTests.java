/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;

/**
 */
public class WatcherShieldAuthorizationFailedRestTests extends WatcherRestTests {

    @Override
    protected boolean enableShield() {
        return true; // Always run with Shield enabled:
    }

    public WatcherShieldAuthorizationFailedRestTests(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Test
    public void test() throws IOException {
        try {
            super.test();
            fail();
        } catch(AssertionError ae) {
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [test]"));
        }
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test", new SecuredString("changeme".toCharArray()));
        return Settings.builder()
                .put(Headers.PREFIX + ".Authorization", token)
                .build();
    }
}
