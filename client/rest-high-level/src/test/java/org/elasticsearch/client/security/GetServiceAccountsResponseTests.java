/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.security.support.ServiceAccountInfo;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class GetServiceAccountsResponseTests
    extends AbstractResponseTestCase<org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse,
    GetServiceAccountsResponse> {

    @Override
    protected org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse createServerTestInstance(
        XContentType xContentType) {
        final String principal = randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8);
        return new org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse(
            new org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo[]{
                new org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo(principal,
                    new RoleDescriptor(principal,
                        randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)),
                        new RoleDescriptor.IndicesPrivileges[]{
                            RoleDescriptor.IndicesPrivileges.builder()
                                .indices(randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)))
                                .privileges(randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)))
                                .build(),
                            RoleDescriptor.IndicesPrivileges.builder()
                                .indices(randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)))
                                .privileges(randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)))
                                .build()
                        },
                        Strings.EMPTY_ARRAY
                    )
                )
            });
    }

    @Override
    protected GetServiceAccountsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetServiceAccountsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse serverTestInstance,
        GetServiceAccountsResponse clientInstance) {
        final org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo serverTestInstanceServiceAccountInfo =
            serverTestInstance.getServiceAccountInfos()[0];
        final String principal = serverTestInstanceServiceAccountInfo.getPrincipal();
        final RoleDescriptor roleDescriptor = serverTestInstanceServiceAccountInfo.getRoleDescriptor();

        assertThat(clientInstance.getServiceAccountInfos().size(), equalTo(1));
        final ServiceAccountInfo serviceAccountInfo = clientInstance.getServiceAccountInfos().get(0);
        assertThat(serviceAccountInfo.getPrincipal(), equalTo(principal));
        assertThat(serviceAccountInfo.getRole(), equalTo(
            Role.builder()
                .name("role_descriptor")
                .clusterPrivileges(roleDescriptor.getClusterPrivileges())
                .indicesPrivileges(
                    IndicesPrivileges.builder()
                        .indices(roleDescriptor.getIndicesPrivileges()[0].getIndices())
                        .privileges(roleDescriptor.getIndicesPrivileges()[0].getPrivileges())
                        .build(),
                    IndicesPrivileges.builder()
                        .indices(roleDescriptor.getIndicesPrivileges()[1].getIndices())
                        .privileges(roleDescriptor.getIndicesPrivileges()[1].getPrivileges())
                        .build()
                )
                .build()
        ));
    }
}
