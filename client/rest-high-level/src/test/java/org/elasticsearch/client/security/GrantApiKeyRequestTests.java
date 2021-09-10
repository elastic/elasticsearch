/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.client.security.user.privileges.Role.ClusterPrivilegeName;
import org.elasticsearch.client.security.user.privileges.Role.IndexPrivilegeName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class GrantApiKeyRequestTests extends ESTestCase {

    public void testToXContent() throws IOException {
        final Map<String, Object> apiKeyMetadata = CreateApiKeyRequestTests.randomMetadata();
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("api-key", List.of(), null, null,
            apiKeyMetadata);
        final GrantApiKeyRequest.Grant grant = GrantApiKeyRequest.Grant.passwordGrant("kamala.khan", "JerseyGirl!".toCharArray());
        final GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest(grant, createApiKeyRequest);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        grantApiKeyRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String output = Strings.toString(builder);
        final String apiKeyMetadataString = apiKeyMetadata == null ? ""
            : ",\"metadata\":" + XContentTestUtils.convertToXContent(apiKeyMetadata, XContentType.JSON).utf8ToString();
        assertThat(output, equalTo(
            "{" +
                "\"grant_type\":\"password\"," +
                "\"username\":\"kamala.khan\"," +
                "\"password\":\"JerseyGirl!\"," +
                "\"api_key\":{\"name\":\"api-key\",\"role_descriptors\":{}" + apiKeyMetadataString + "}" +
                "}"));
    }

    public void testEqualsHashCode() {
        final String name = randomAlphaOfLength(5);
        List<Role> roles = randomList(1, 3, () ->
            Role.builder()
                .name(randomAlphaOfLengthBetween(3, 8))
                .clusterPrivileges(randomSubsetOf(randomIntBetween(1, 3), ClusterPrivilegeName.ALL_ARRAY))
                .indicesPrivileges(
                    IndicesPrivileges
                        .builder()
                        .indices(randomAlphaOfLengthBetween(4, 12))
                        .privileges(randomSubsetOf(randomIntBetween(1, 3), IndexPrivilegeName.ALL_ARRAY))
                        .build()
                ).build()
        );
        final TimeValue expiration = randomBoolean() ? null : TimeValue.timeValueHours(randomIntBetween(4, 100));
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy,
            CreateApiKeyRequestTests.randomMetadata());
        final GrantApiKeyRequest.Grant grant = randomBoolean()
            ? GrantApiKeyRequest.Grant.passwordGrant(randomAlphaOfLength(8), randomAlphaOfLengthBetween(6, 12).toCharArray())
            : GrantApiKeyRequest.Grant.accessTokenGrant(randomAlphaOfLength(24));

        final GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest(grant, createApiKeyRequest);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            grantApiKeyRequest,
            original -> new GrantApiKeyRequest(clone(original.getGrant()), clone(original.getApiKeyRequest())),
            GrantApiKeyRequestTests::mutateTestItem);
    }

    private GrantApiKeyRequest.Grant clone(GrantApiKeyRequest.Grant grant) {
        switch (grant.getGrantType()) {
            case "password":
                return GrantApiKeyRequest.Grant.passwordGrant(grant.getUsername(), grant.getPassword());
            case "access_token":
                return GrantApiKeyRequest.Grant.accessTokenGrant(grant.getAccessToken());
            default:
                throw new IllegalArgumentException("Cannot clone grant: " + Strings.toString(grant));
        }
    }

    private CreateApiKeyRequest clone(CreateApiKeyRequest apiKeyRequest) {
        return new CreateApiKeyRequest(
            apiKeyRequest.getName(),
            apiKeyRequest.getRoles().stream().map(r -> Role.builder().clone(r).build()).collect(Collectors.toUnmodifiableList()),
            apiKeyRequest.getExpiration(),
            apiKeyRequest.getRefreshPolicy(),
            apiKeyRequest.getMetadata()
        );
    }

    private static GrantApiKeyRequest mutateTestItem(GrantApiKeyRequest original) {
        switch (randomIntBetween(0, 3)) {
            case 0:
                return new GrantApiKeyRequest(original.getGrant().getGrantType().equals("password")
                    ? GrantApiKeyRequest.Grant.accessTokenGrant(randomAlphaOfLength(24))
                    : GrantApiKeyRequest.Grant.passwordGrant(randomAlphaOfLength(8), randomAlphaOfLengthBetween(6, 12).toCharArray()),
                    original.getApiKeyRequest());
            case 1:
                return new GrantApiKeyRequest(original.getGrant(),
                    new CreateApiKeyRequest(
                        randomAlphaOfLengthBetween(10, 15),
                        original.getApiKeyRequest().getRoles(),
                        original.getApiKeyRequest().getExpiration(),
                        original.getApiKeyRequest().getRefreshPolicy(),
                        original.getApiKeyRequest().getMetadata()
                    )
                );
            case 2:
                return new GrantApiKeyRequest(original.getGrant(),
                    new CreateApiKeyRequest(
                        original.getApiKeyRequest().getName(),
                        List.of(), // No role limits
                        original.getApiKeyRequest().getExpiration(),
                        original.getApiKeyRequest().getRefreshPolicy(),
                        original.getApiKeyRequest().getMetadata()
                    )
                );
            case 3:
                return new GrantApiKeyRequest(original.getGrant(),
                    new CreateApiKeyRequest(
                        original.getApiKeyRequest().getName(),
                        original.getApiKeyRequest().getRoles(),
                        original.getApiKeyRequest().getExpiration(),
                        original.getApiKeyRequest().getRefreshPolicy(),
                        randomValueOtherThan(original.getApiKeyRequest().getMetadata(), CreateApiKeyRequestTests::randomMetadata)
                    )
                );
            default:
                return new GrantApiKeyRequest(original.getGrant(),
                    new CreateApiKeyRequest(
                        original.getApiKeyRequest().getName(),
                        original.getApiKeyRequest().getRoles(),
                        TimeValue.timeValueMinutes(randomIntBetween(10, 120)),
                        original.getApiKeyRequest().getRefreshPolicy(),
                        original.getApiKeyRequest().getMetadata()
                    )
                );
        }
    }
}
