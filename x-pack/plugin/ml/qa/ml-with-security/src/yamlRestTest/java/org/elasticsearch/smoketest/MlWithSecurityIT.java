/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.xpack.test.rest.AbstractXPackRestTest;
import org.junit.ClassRule;

import java.util.Collections;
import java.util.Map;

public class MlWithSecurityIT extends AbstractXPackRestTest {

    private static final String TEST_ADMIN_USERNAME = "x_pack_rest_user";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(TEST_ADMIN_USERNAME, "x-pack-test-password")
        .user("ml_admin", "x-pack-test-password", "minimal,machine_learning_admin,ingest_admin", false)
        .user("ml_user", "x-pack-test-password", "minimal,machine_learning_user", false)
        .user("no_ml", "x-pack-test-password", "minimal", false)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .systemProperty("es.queryable_built_in_roles_enabled", "false")
        .build();

    public MlWithSecurityIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected String[] getCredentials() {
        return new String[] { "ml_admin", "x-pack-test-password" };
    }

    @Override
    protected Settings restClientSettings() {
        String[] creds = getCredentials();
        String token = basicAuthHeaderValue(creds[0], new SecureString(creds[1].toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Map<String, String> getApiCallHeaders() {
        return Collections.singletonMap(
            "Authorization",
            basicAuthHeaderValue(TEST_ADMIN_USERNAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
    }

    @Override
    protected boolean isMachineLearningTest() {
        return true;
    }
}
