/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

public class GetServiceAccountResponseTests extends AbstractWireSerializingTestCase<GetServiceAccountResponse> {

    @Override
    protected Writeable.Reader<GetServiceAccountResponse> instanceReader() {
        return GetServiceAccountResponse::new;
    }

    @Override
    protected GetServiceAccountResponse createTestInstance() {
        final String principal = randomPrincipal();
        return new GetServiceAccountResponse(
            randomBoolean()
                ? new ServiceAccountInfo[] { new ServiceAccountInfo(principal, getRoleDescriptorFor(principal)) }
                : new ServiceAccountInfo[0]
        );
    }

    @Override
    protected GetServiceAccountResponse mutateInstance(GetServiceAccountResponse instance) {
        if (instance.getServiceAccountInfos().length == 0) {
            final String principal = randomPrincipal();
            return new GetServiceAccountResponse(
                new ServiceAccountInfo[] { new ServiceAccountInfo(principal, getRoleDescriptorFor(principal)) }
            );
        } else {
            return new GetServiceAccountResponse(new ServiceAccountInfo[0]);
        }
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        final GetServiceAccountResponse response = createTestInstance();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final Map<String, Object> responseMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();
        final ServiceAccountInfo[] serviceAccountInfos = response.getServiceAccountInfos();
        if (serviceAccountInfos.length == 0) {
            assertThat(responseMap, anEmptyMap());
        } else {
            assertThat(responseMap.size(), equalTo(serviceAccountInfos.length));
            for (int i = 0; i < serviceAccountInfos.length - 1; i++) {
                final String key = serviceAccountInfos[i].getPrincipal();
                assertRoleDescriptorEquals((Map<String, Object>) responseMap.get(key), serviceAccountInfos[i].getRoleDescriptor());
            }
        }
    }

    private String randomPrincipal() {
        return randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8);
    }

    private RoleDescriptor getRoleDescriptorFor(String name) {
        return new RoleDescriptor(
            name,
            new String[] { "monitor", "manage_own_api_key" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs-*", "metrics-*", "traces-*")
                    .privileges("write", "create_index", "auto_configure")
                    .build() },
            null,
            null,
            null,
            null,
            null
        );
    }

    private void assertRoleDescriptorEquals(Map<String, Object> responseFragment, RoleDescriptor roleDescriptor) throws IOException {
        @SuppressWarnings("unchecked")
        final Map<String, Object> descriptorMap = (Map<String, Object>) responseFragment.get("role_descriptor");
        assertThat(
            RoleDescriptor.parse(
                roleDescriptor.getName(),
                XContentTestUtils.convertToXContent(descriptorMap, XContentType.JSON),
                false,
                XContentType.JSON
            ),
            equalTo(roleDescriptor)
        );
    }
}
