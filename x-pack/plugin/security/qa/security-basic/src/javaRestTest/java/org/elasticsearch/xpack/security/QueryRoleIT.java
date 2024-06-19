/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class QueryRoleIT extends SecurityInBasicRestTestCase {

    private static final String READ_SECURITY_USER_AUTH_HEADER = "Basic cmVhZF9zZWN1cml0eV91c2VyOnJlYWQtc2VjdXJpdHktcGFzc3dvcmQ=";
    private static final String TEST_USER_NO_READ_USERS_AUTH_HEADER = "Basic c2VjdXJpdHlfdGVzdF91c2VyOnNlY3VyaXR5LXRlc3QtcGFzc3dvcmQ=";

    public void testX() throws IOException {
        RoleDescriptor x = createRole("x", "description", Map.of("m1", "v1"), new ApplicationResourcePrivileges[0]);
        RoleDescriptor y = createRole("y", "description", Map.of("m1", "v1"), new ApplicationResourcePrivileges[0]);
    }

    private RoleDescriptor createRole(
        String roleName,
        String description,
        Map<String, Object> metadata,
        ApplicationResourcePrivileges... applicationResourcePrivileges
    ) throws IOException {
        Request request = new Request("POST", "/_security/role/" + roleName);
        BytesReference source = BytesReference.bytes(
            jsonBuilder().map(
                Map.of(
                    RoleDescriptor.Fields.DESCRIPTION.getPreferredName(),
                    description,
                    RoleDescriptor.Fields.METADATA.getPreferredName(),
                    metadata,
                    RoleDescriptor.Fields.APPLICATIONS.getPreferredName(),
                    applicationResourcePrivileges
                )
            )
        );
        request.setJsonEntity(source.utf8ToString());
        Response response = adminClient().performRequest(request);
        assertOK(response);
        assertTrue((boolean) responseAsMap(response).get("created"));
        return new RoleDescriptor(
            roleName,
            null,
            null,
            applicationResourcePrivileges,
            null,
            null,
            metadata,
            null,
            null,
            null,
            null,
            description
        );
    }
}
