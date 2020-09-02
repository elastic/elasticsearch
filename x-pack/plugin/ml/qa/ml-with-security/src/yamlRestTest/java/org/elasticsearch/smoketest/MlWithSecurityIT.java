/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.xpack.test.rest.XPackRestIT;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;


public class MlWithSecurityIT extends XPackRestIT {

    private static final String TEST_ADMIN_USERNAME = "x_pack_rest_user";

    public MlWithSecurityIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    protected String[] getCredentials() {
        return new String[]{"ml_admin", "x-pack-test-password"};
    }

    @Override
    protected Settings restClientSettings() {
        String[] creds = getCredentials();
        String token = basicAuthHeaderValue(creds[0], new SecureString(creds[1].toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    protected Map<String, String> getApiCallHeaders() {
        return Collections.singletonMap("Authorization", basicAuthHeaderValue(TEST_ADMIN_USERNAME,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
    }

    @Override
    protected boolean isMonitoringTest() {
        return false;
    }

    @Override
    protected boolean isMachineLearningTest() {
        return true;
    }
}
