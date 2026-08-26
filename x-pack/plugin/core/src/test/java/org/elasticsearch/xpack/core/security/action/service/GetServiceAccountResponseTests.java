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
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

public class GetServiceAccountResponseTests extends AbstractWireSerializingTestCase<GetServiceAccountResponse> {

    @Override
    protected Writeable.Reader<GetServiceAccountResponse> instanceReader() {
        return GetServiceAccountResponse::new;
    }

    @Override
    protected GetServiceAccountResponse createTestInstance() {
        return new GetServiceAccountResponse(randomServiceAccountInfos(randomIntBetween(0, 3)));
    }

    @Override
    protected GetServiceAccountResponse mutateInstance(GetServiceAccountResponse instance) {
        final int length = instance.getServiceAccountInfos().length;
        return new GetServiceAccountResponse(randomServiceAccountInfos(randomValueOtherThan(length, () -> randomIntBetween(0, 3))));
    }

    public void testToXContentReportsEveryAccountWithHowItIsManaged() throws IOException {
        final RoleDescriptor roleDescriptor = getRoleDescriptorFor("elastic/fleet-server");
        final GetServiceAccountResponse response = new GetServiceAccountResponse(
            new ServiceAccountInfo[] {
                new ServiceAccountInfo.BuiltIn("elastic/fleet-server", roleDescriptor),
                new ServiceAccountInfo.UserManaged("my-team/worker", List.of("role-a", "role-b"), false) }
        );

        final Map<String, Object> responseMap = toMap(response);

        assertThat(responseMap.size(), equalTo(2));
        final Map<String, Object> builtIn = fragment(responseMap, "elastic/fleet-server");
        assertThat(builtIn.get("managed_by"), equalTo("elastic"));
        assertRoleDescriptorEquals(builtIn, roleDescriptor);
        assertThat(
            fragment(responseMap, "my-team/worker"),
            equalTo(Map.of("managed_by", "user", "roles", List.of("role-a", "role-b"), "enabled", false))
        );
    }

    public void testToXContentOfNoAccountsIsAnEmptyObject() throws IOException {
        assertThat(toMap(new GetServiceAccountResponse(new ServiceAccountInfo[0])), anEmptyMap());
    }

    private ServiceAccountInfo[] randomServiceAccountInfos(int count) {
        // Principals are distinct because a response renders each account as a field named for its principal.
        return IntStream.range(0, count).mapToObj(i -> randomServiceAccountInfo("ns" + i + "/svc" + i)).toArray(ServiceAccountInfo[]::new);
    }

    private ServiceAccountInfo randomServiceAccountInfo(String principal) {
        return randomBoolean()
            ? new ServiceAccountInfo.BuiltIn(principal, getRoleDescriptorFor(principal))
            : new ServiceAccountInfo.UserManaged(principal, randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8)), randomBoolean());
    }

    private static Map<String, Object> toMap(GetServiceAccountResponse response) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> fragment(Map<String, Object> responseMap, String principal) {
        return (Map<String, Object>) responseMap.get(principal);
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
            RoleDescriptor.parserBuilder()
                .build()
                .parse(roleDescriptor.getName(), XContentTestUtils.convertToXContent(descriptorMap, XContentType.JSON), XContentType.JSON),
            equalTo(roleDescriptor)
        );
    }
}
