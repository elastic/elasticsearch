/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.support.SecurityMigrations;
import org.junit.BeforeClass;

import static org.elasticsearch.xpack.security.QueryRoleIT.assertQuery;
import static org.elasticsearch.xpack.security.QueryRoleIT.waitForMigrationCompletion;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.oneOf;

public class QueryableReservedRolesIT extends SecurityInBasicRestTestCase {

    @BeforeClass
    public static void setup() {
        new ReservedRolesStore();
    }

    public void testQueryDeleteOrUpdateReservedRoles() throws Exception {
        waitForMigrationCompletion(adminClient(), SecurityMigrations.ROLE_METADATA_FLATTENED_MIGRATION_VERSION);

        final String[] allReservedRoles = ReservedRolesStore.names().toArray(new String[0]);
        assertQuery("""
            { "query": { "bool": { "must": { "term": { "metadata._reserved": true } } } }, "size": 100 }
            """, 31, roles -> {
            assertThat(roles, iterableWithSize(31));
            for (var role : roles) {
                assertThat((String) role.get("name"), is(oneOf(allReservedRoles)));
            }
        });

        final String roleName = randomFrom(allReservedRoles);
        assertQuery(String.format("""
            { "query": { "bool": { "must": { "term": { "name": "%s" } } } } }
            """, roleName), 1, roles -> {
            assertThat(roles, iterableWithSize(1));
            assertThat((String) roles.get(0).get("name"), equalTo(roleName));
        });

        assertDeleteReservedRole(roleName);
        assertCreateOrUpdateReservedRole(roleName);
    }

    private void assertDeleteReservedRole(String roleName) throws Exception {
        Request request = new Request("DELETE", "/_security/role/" + roleName);
        var e = expectThrows(ResponseException.class, () -> adminClient().performRequest(request));
        assertThat(e.getMessage(), containsString("role [" + roleName + "] is reserved and cannot be deleted"));
    }

    private void assertCreateOrUpdateReservedRole(String roleName) throws Exception {
        Request request = new Request(randomBoolean() ? "PUT" : "POST", "/_security/role/" + roleName);
        request.setJsonEntity("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all"]
                }
              ]
            }
            """);
        var e = expectThrows(ResponseException.class, () -> adminClient().performRequest(request));
        assertThat(e.getMessage(), containsString("Role [" + roleName + "] is reserved and may not be used."));
    }

}
