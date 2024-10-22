/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;

public class SecurityAuthorizationIT extends SecurityIntegTestCase {

    private static final RoleDescriptor ROLE_DESCRIPTOR = new RoleDescriptor(
        "my_admin_role",
        new String[] { "all" },
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .privileges("all")
                .indices("*")
                .allowRestrictedIndices(false)
                .grantedFields("*")
                .build() },
        null
    );

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    public void test() throws Exception {
        getSecurityClient().putUser(new User("my_admin_user", ROLE_DESCRIPTOR.getName()), TEST_PASSWORD_SECURE_STRING);
        getSecurityClient().putRole(ROLE_DESCRIPTOR);
        ensureGreen(INTERNAL_SECURITY_MAIN_INDEX_7);

        String indexName = ".security-*";
        Request request = new Request("GET", "_cat/shards/" + indexName);
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue("my_admin_user", TEST_PASSWORD_SECURE_STRING))
        );
        try (var restClient = createRestClient()) {
            String response = EntityUtils.toString(restClient.performRequest(request).getEntity());
            assertThat(response, is(emptyString()));
        }
    }
}
