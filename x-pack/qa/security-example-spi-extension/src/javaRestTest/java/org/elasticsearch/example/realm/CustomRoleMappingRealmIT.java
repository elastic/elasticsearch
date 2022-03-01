/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration test to test authentication with the custom role-mapping realm
 */
public class CustomRoleMappingRealmIT extends ESRestTestCase {

    private String expectedRole;

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + "." + CustomRealm.USER_HEADER, CustomRealmIT.USERNAME)
            .put(ThreadContext.PREFIX + "." + CustomRealm.PW_HEADER, CustomRealmIT.PASSWORD)
            .build();
    }

    @Before
    public void setupRoleMapping() throws Exception {
        expectedRole = randomAlphaOfLengthBetween(4, 16);
        Request request = new Request("PUT", "/_security/role_mapping/test");
        request.setJsonEntity("""
            {
              "enabled": true,
              "roles": [ "%s" ],
              "rules": {
                "field": {
                  "groups": "%s"
                }
              }
            }""".formatted(expectedRole, CustomRoleMappingRealm.USER_GROUP));
        adminClient().performRequest(request);
    }

    public void testUserWithRoleMapping() throws Exception {
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        // Authenticate as the custom realm superuser
        options.addHeader(CustomRealm.USER_HEADER, CustomRealmIT.USERNAME);
        options.addHeader(CustomRealm.PW_HEADER, CustomRealmIT.PASSWORD);
        // But "run-as" the role mapped user
        options.addHeader("es-security-runas-user", CustomRoleMappingRealm.USERNAME);
        request.setOptions(options);

        final Response response = client().performRequest(request);
        final Map<String, Object> authenticate = entityAsMap(response);
        assertThat(authenticate.get("username"), Matchers.is(CustomRoleMappingRealm.USERNAME));
        assertThat(authenticate.get("roles"), instanceOf(List.class));
        assertThat(authenticate.get("roles"), equalTo(List.of(expectedRole)));
    }

}
