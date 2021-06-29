/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivileges;
import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivilegesTests;
import org.elasticsearch.client.security.user.privileges.GlobalOperationPrivilege;
import org.elasticsearch.client.security.user.privileges.GlobalPrivileges;
import org.elasticsearch.client.security.user.privileges.GlobalPrivilegesTests;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.IndicesPrivilegesTests;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.empty;

public class PutRoleRequestTests extends AbstractXContentTestCase<PutRoleRequest> {

    private static final String roleName = "testRoleName";

    @Override
    protected PutRoleRequest createTestInstance() {
        final Role role = randomRole(roleName);
        return new PutRoleRequest(role, null);
    }

    @Override
    protected PutRoleRequest doParseInstance(XContentParser parser) throws IOException {
        final Tuple<Role, Map<String, Object>> roleAndTransientMetadata = Role.fromXContent(parser, roleName);
        assertThat(roleAndTransientMetadata.v2().entrySet(), is(empty()));
        return new PutRoleRequest(roleAndTransientMetadata.v1(), null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    private static Role randomRole(String roleName) {
        final Role.Builder roleBuilder = Role.builder().name(roleName)
                .clusterPrivileges(randomSubsetOf(randomInt(3), Role.ClusterPrivilegeName.ALL_ARRAY))
                .indicesPrivileges(
                        randomArray(3, IndicesPrivileges[]::new, () -> IndicesPrivilegesTests.createNewRandom(randomAlphaOfLength(3))))
                .applicationResourcePrivileges(randomArray(3, ApplicationResourcePrivileges[]::new,
                        () -> ApplicationResourcePrivilegesTests.createNewRandom(randomAlphaOfLength(3).toLowerCase(Locale.ROOT))))
                .runAsPrivilege(randomArray(3, String[]::new, () -> randomAlphaOfLength(3)));
        if (randomBoolean()) {
            roleBuilder.globalApplicationPrivileges(new GlobalPrivileges(Arrays.asList(
                    randomArray(1, 3, GlobalOperationPrivilege[]::new, () -> GlobalPrivilegesTests.buildRandomGlobalScopedPrivilege()))));
        }
        if (randomBoolean()) {
            final Map<String, Object> metadata = new HashMap<>();
            for (int i = 0; i < randomInt(3); i++) {
                metadata.put(randomAlphaOfLength(3), randomAlphaOfLength(3));
            }
            roleBuilder.metadata(metadata);
        }
        return roleBuilder.build();
    }

}
