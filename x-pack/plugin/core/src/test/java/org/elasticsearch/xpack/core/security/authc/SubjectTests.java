/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference.ApiKeyRoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference.BwcApiKeyRoleReference;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class SubjectTests extends ESTestCase {

    public void testGetRoleReferencesForApiKey() {
        Map<String, Object> authMetadata = new HashMap<>();
        final String apiKeyId = randomAlphaOfLength(12);
        authMetadata.put(AuthenticationField.API_KEY_ID_KEY, apiKeyId);
        authMetadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLength(12));
        final BytesReference roleBytes = new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}");
        final BytesReference limitedByRoleBytes = new BytesArray("{\"limitedBy role\": {\"cluster\": [\"all\"]}}");

        final boolean emptyRoleBytes = randomBoolean();

        authMetadata.put(
            AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
            emptyRoleBytes ? randomFrom(Arrays.asList(null, new BytesArray("{}"))) : roleBytes
        );
        authMetadata.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, limitedByRoleBytes);

        final Subject subject = new Subject(
            new User("joe"),
            new Authentication.RealmRef(API_KEY_REALM_NAME, API_KEY_REALM_TYPE, "node"),
            Version.CURRENT,
            authMetadata
        );

        final List<RoleReference> roleReferences = subject.getRoleReferences(null);
        if (emptyRoleBytes) {
            assertThat(roleReferences, contains(isA(ApiKeyRoleReference.class)));
            final ApiKeyRoleReference roleReference = (ApiKeyRoleReference) roleReferences.get(0);
            assertThat(roleReference.getApiKeyId(), equalTo(apiKeyId));
            assertThat(roleReference.getRoleDescriptorsBytes(), equalTo(authMetadata.get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)));
        } else {
            assertThat(roleReferences, contains(isA(ApiKeyRoleReference.class), isA(ApiKeyRoleReference.class)));
            final ApiKeyRoleReference roleReference = (ApiKeyRoleReference) roleReferences.get(0);
            assertThat(roleReference.getApiKeyId(), equalTo(apiKeyId));
            assertThat(roleReference.getRoleDescriptorsBytes(), equalTo(authMetadata.get(API_KEY_ROLE_DESCRIPTORS_KEY)));

            final ApiKeyRoleReference limitedByRoleReference = (ApiKeyRoleReference) roleReferences.get(1);
            assertThat(limitedByRoleReference.getApiKeyId(), equalTo(apiKeyId));
            assertThat(limitedByRoleReference.getRoleDescriptorsBytes(), equalTo(authMetadata.get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)));
        }
    }

    public void testGetRoleReferencesForApiKeyBwc() {
        Map<String, Object> authMetadata = new HashMap<>();
        final String apiKeyId = randomAlphaOfLength(12);
        authMetadata.put(AuthenticationField.API_KEY_ID_KEY, apiKeyId);
        authMetadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLength(12));
        boolean emptyApiKeyRoleDescriptor = randomBoolean();
        Map<String, Object> roleARDMap = Map.of("cluster", List.of("monitor"));
        authMetadata.put(
            API_KEY_ROLE_DESCRIPTORS_KEY,
            (emptyApiKeyRoleDescriptor)
                ? randomFrom(Arrays.asList(null, Collections.emptyMap()))
                : Collections.singletonMap("a role", roleARDMap)
        );

        Map<String, Object> limitedRdMap = Map.of("cluster", List.of("all"));
        authMetadata.put(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, Collections.singletonMap("limited role", limitedRdMap));

        final Subject subject = new Subject(
            new User("joe"),
            new Authentication.RealmRef(API_KEY_REALM_NAME, API_KEY_REALM_TYPE, "node"),
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_8_1),
            authMetadata
        );

        final List<RoleReference> roleReferences = subject.getRoleReferences(null);

        if (emptyApiKeyRoleDescriptor) {
            assertThat(roleReferences, contains(isA(BwcApiKeyRoleReference.class)));
            final BwcApiKeyRoleReference limitedByRoleReference = (BwcApiKeyRoleReference) roleReferences.get(0);
            assertThat(limitedByRoleReference.getApiKeyId(), equalTo(apiKeyId));
            assertThat(limitedByRoleReference.getRoleDescriptorsMap(), equalTo(authMetadata.get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)));
        } else {
            assertThat(roleReferences, contains(isA(BwcApiKeyRoleReference.class), isA(BwcApiKeyRoleReference.class)));
            final BwcApiKeyRoleReference roleReference = (BwcApiKeyRoleReference) roleReferences.get(0);
            assertThat(roleReference.getApiKeyId(), equalTo(apiKeyId));
            assertThat(roleReference.getRoleDescriptorsMap(), equalTo(authMetadata.get(API_KEY_ROLE_DESCRIPTORS_KEY)));

            final BwcApiKeyRoleReference limitedByRoleReference = (BwcApiKeyRoleReference) roleReferences.get(1);
            assertThat(limitedByRoleReference.getApiKeyId(), equalTo(apiKeyId));
            assertThat(limitedByRoleReference.getRoleDescriptorsMap(), equalTo(authMetadata.get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)));
        }
    }

}
