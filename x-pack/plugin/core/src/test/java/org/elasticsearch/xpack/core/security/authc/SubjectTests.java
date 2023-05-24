/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.store.RoleKey;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference.ApiKeyRoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference.BwcApiKeyRoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference.FixedRoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference.NamedRoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference.ServiceAccountRoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReferenceIntersection;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.CROSS_CLUSTER_ACCESS_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.CROSS_CLUSTER_ACCESS_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.Subject.FLEET_SERVER_ROLE_DESCRIPTOR_BYTES_V_7_14;
import static org.elasticsearch.xpack.core.security.authz.store.RoleReference.CrossClusterAccessRoleReference;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;

public class SubjectTests extends ESTestCase {

    public void testGetRoleReferencesForRegularUser() {
        final User user = new User("joe", "role_a", "role_b");
        final Subject subject = new Subject(
            user,
            new Authentication.RealmRef(randomAlphaOfLength(5), randomAlphaOfLength(5), "node"),
            TransportVersion.CURRENT,
            Map.of()
        );

        final AnonymousUser anonymousUser = randomFrom(getAnonymousUser(), null);

        final RoleReferenceIntersection roleReferenceIntersection = subject.getRoleReferenceIntersection(anonymousUser);
        final List<RoleReference> roleReferences = roleReferenceIntersection.getRoleReferences();
        assertThat(roleReferences, hasSize(1));
        assertThat(roleReferences.get(0), instanceOf(NamedRoleReference.class));
        final NamedRoleReference namedRoleReference = (NamedRoleReference) roleReferences.get(0);
        assertThat(
            namedRoleReference.getRoleNames(),
            arrayContaining(ArrayUtils.concat(user.roles(), anonymousUser == null ? Strings.EMPTY_ARRAY : anonymousUser.roles()))
        );
    }

    public void testGetRoleReferencesForAnonymousUser() {
        final AnonymousUser anonymousUser = getAnonymousUser();

        final Subject subject = new Subject(
            anonymousUser,
            new Authentication.RealmRef(randomAlphaOfLength(5), randomAlphaOfLength(5), "node"),
            TransportVersion.CURRENT,
            Map.of()
        );

        final RoleReferenceIntersection roleReferenceIntersection = subject.getRoleReferenceIntersection(anonymousUser);
        final List<RoleReference> roleReferences = roleReferenceIntersection.getRoleReferences();
        assertThat(roleReferences, hasSize(1));
        assertThat(roleReferences.get(0), instanceOf(NamedRoleReference.class));
        final NamedRoleReference namedRoleReference = (NamedRoleReference) roleReferences.get(0);
        // Anonymous roles do not get applied again
        assertThat(namedRoleReference.getRoleNames(), equalTo(anonymousUser.roles()));
    }

    public void testGetRoleReferencesForServiceAccount() {
        final User serviceUser = new User(randomAlphaOfLength(5) + "/" + randomAlphaOfLength(5));
        final Subject subject = new Subject(
            serviceUser,
            new Authentication.RealmRef(ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE, "node"),
            TransportVersion.CURRENT,
            Map.of()
        );

        final RoleReferenceIntersection roleReferenceIntersection = subject.getRoleReferenceIntersection(getAnonymousUser());
        final List<RoleReference> roleReferences = roleReferenceIntersection.getRoleReferences();
        assertThat(roleReferences, hasSize(1));
        assertThat(roleReferences.get(0), instanceOf(ServiceAccountRoleReference.class));
        final ServiceAccountRoleReference serviceAccountRoleReference = (ServiceAccountRoleReference) roleReferences.get(0);
        assertThat(serviceAccountRoleReference.getPrincipal(), equalTo(serviceUser.principal()));
    }

    public void testGetRoleReferencesForRestApiKey() {
        Map<String, Object> authMetadata = new HashMap<>();
        final String apiKeyId = randomAlphaOfLength(12);
        authMetadata.put(AuthenticationField.API_KEY_ID_KEY, apiKeyId);
        authMetadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLength(12));
        authMetadata.put(AuthenticationField.API_KEY_TYPE_KEY, ApiKey.Type.REST.value());
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
            TransportVersion.CURRENT,
            authMetadata
        );

        final RoleReferenceIntersection roleReferenceIntersection = subject.getRoleReferenceIntersection(getAnonymousUser());
        final List<RoleReference> roleReferences = roleReferenceIntersection.getRoleReferences();
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

    public void testBuildRoleReferenceForCrossClusterApiKey() {
        Map<String, Object> authMetadata = new HashMap<>();
        final String apiKeyId = randomAlphaOfLength(12);
        authMetadata.put(AuthenticationField.API_KEY_ID_KEY, apiKeyId);
        authMetadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLength(12));
        authMetadata.put(AuthenticationField.API_KEY_TYPE_KEY, ApiKey.Type.CROSS_CLUSTER.value());
        final BytesReference roleBytes = new BytesArray("""
            {
              "cross_cluster": {
                "cluster": ["cross_cluster_search"],
                "indices": [
                  { "names":["index"], "privileges":["read","read_cross_cluster","view_index_metadata"] }
                ]
              }
            }""");
        authMetadata.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, roleBytes);
        authMetadata.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, new BytesArray("{}"));

        final Subject subject = new Subject(
            new User("joe"),
            new Authentication.RealmRef(API_KEY_REALM_NAME, API_KEY_REALM_TYPE, "node"),
            TransportVersion.CURRENT,
            authMetadata
        );

        final ApiKeyRoleReference roleReference = subject.buildRoleReferenceForCrossClusterApiKey();
        assertThat(roleReference.getApiKeyId(), equalTo(apiKeyId));
        assertThat(roleReference.getRoleDescriptorsBytes(), equalTo(authMetadata.get(API_KEY_ROLE_DESCRIPTORS_KEY)));
    }

    public void testGetRoleReferencesForCrossClusterAccess() {
        Map<String, Object> authMetadata = new HashMap<>();
        final String apiKeyId = randomAlphaOfLength(12);
        authMetadata.put(AuthenticationField.API_KEY_ID_KEY, apiKeyId);
        authMetadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLength(12));
        authMetadata.put(AuthenticationField.API_KEY_TYPE_KEY, ApiKey.Type.CROSS_CLUSTER.value());
        final BytesReference roleBytes = new BytesArray("""
            {
              "cross_cluster": {
                "cluster": ["cross_cluster_replication"],
                "indices": [
                  { "names":["index*"],"privileges":["cross_cluster_replication","cross_cluster_replication_internal"] }
                ]
              }
            }""");

        authMetadata.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, roleBytes);
        authMetadata.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, new BytesArray("{}"));

        final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo = randomBoolean()
            ? AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(RoleDescriptorsIntersection.EMPTY)
            : AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo();
        authMetadata = crossClusterAccessSubjectInfo.copyWithCrossClusterAccessEntries(authMetadata);

        final Subject subject = new Subject(
            new User("joe"),
            new Authentication.RealmRef(CROSS_CLUSTER_ACCESS_REALM_NAME, CROSS_CLUSTER_ACCESS_REALM_TYPE, "node"),
            TransportVersion.CURRENT,
            authMetadata
        );

        final Authentication authentication = crossClusterAccessSubjectInfo.getAuthentication();
        final boolean isInternalUser = authentication.getEffectiveSubject().getUser() == InternalUsers.CROSS_CLUSTER_ACCESS_USER;
        final RoleReferenceIntersection roleReferenceIntersection = subject.getRoleReferenceIntersection(getAnonymousUser());
        // Number of role references depends on the authentication and its number of roles.
        // Test setup can randomly authentication with 0, 1 or 2 (in case of API key) role descriptors,
        final int numberOfRemoteRoleDescriptors = crossClusterAccessSubjectInfo.getRoleDescriptorsBytesList().size();
        assertThat(numberOfRemoteRoleDescriptors, anyOf(equalTo(0), equalTo(1), equalTo(2)));
        final List<RoleReference> roleReferences = roleReferenceIntersection.getRoleReferences();
        if (numberOfRemoteRoleDescriptors == 2) {
            // Two role references means we can't have a FixedRoleReference
            assertThat(
                roleReferences,
                contains(
                    isA(CrossClusterAccessRoleReference.class),
                    isA(CrossClusterAccessRoleReference.class),
                    isA(ApiKeyRoleReference.class)
                )
            );

            expectCrossClusterAccessReferenceAtIndex(0, roleReferences, crossClusterAccessSubjectInfo);
            expectCrossClusterAccessReferenceAtIndex(1, roleReferences, crossClusterAccessSubjectInfo);

            final ApiKeyRoleReference roleReference = (ApiKeyRoleReference) roleReferences.get(2);
            assertThat(roleReference.getApiKeyId(), equalTo(apiKeyId));
            assertThat(roleReference.getRoleDescriptorsBytes(), equalTo(authMetadata.get(API_KEY_ROLE_DESCRIPTORS_KEY)));
        } else {
            if (isInternalUser) {
                assertThat(roleReferences, contains(isA(FixedRoleReference.class), isA(ApiKeyRoleReference.class)));
                expectFixedReferenceAtIndex(0, roleReferences);
            } else {
                assertThat(roleReferences, contains(isA(CrossClusterAccessRoleReference.class), isA(ApiKeyRoleReference.class)));
                expectCrossClusterAccessReferenceAtIndex(0, roleReferences, crossClusterAccessSubjectInfo);
            }

            final ApiKeyRoleReference roleReference = (ApiKeyRoleReference) roleReferences.get(1);
            assertThat(roleReference.getApiKeyId(), equalTo(apiKeyId));
            assertThat(roleReference.getRoleDescriptorsBytes(), equalTo(authMetadata.get(API_KEY_ROLE_DESCRIPTORS_KEY)));
        }
    }

    private static void expectCrossClusterAccessReferenceAtIndex(
        int index,
        List<RoleReference> roleReferences,
        CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo
    ) {
        final CrossClusterAccessRoleReference reference = (CrossClusterAccessRoleReference) roleReferences.get(index);
        assertThat(
            reference.getRoleDescriptorsBytes(),
            equalTo(
                crossClusterAccessSubjectInfo.getRoleDescriptorsBytesList().isEmpty()
                    ? CrossClusterAccessSubjectInfo.RoleDescriptorsBytes.EMPTY
                    : crossClusterAccessSubjectInfo.getRoleDescriptorsBytesList().get(index)
            )
        );
    }

    private static void expectFixedReferenceAtIndex(int index, List<RoleReference> roleReferences) {
        final FixedRoleReference fixedRoleReference = (FixedRoleReference) roleReferences.get(index);
        final RoleKey expectedKey = new RoleKey(
            Set.of(InternalUsers.CROSS_CLUSTER_ACCESS_USER.getRemoteAccessRoleDescriptor().get().getName()),
            "cross_cluster_access_internal"
        );
        assertThat(fixedRoleReference.id(), equalTo(expectedKey));
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
            TransportVersionUtils.randomVersionBetween(random(), TransportVersion.V_7_0_0, TransportVersion.V_7_8_1),
            authMetadata
        );

        final RoleReferenceIntersection roleReferenceIntersection = subject.getRoleReferenceIntersection(getAnonymousUser());
        final List<RoleReference> roleReferences = roleReferenceIntersection.getRoleReferences();

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

    public void testGetFleetApiKeyRoleReferenceBwcBugFix() {
        final BytesReference roleBytes = new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}");
        final BytesReference limitedByRoleBytes = new BytesArray("{}");
        final Subject subject = new Subject(
            new User("elastic/fleet-server"),
            new Authentication.RealmRef(API_KEY_REALM_NAME, API_KEY_REALM_TYPE, "node"),
            TransportVersion.CURRENT,
            Map.of(
                AuthenticationField.API_KEY_CREATOR_REALM_NAME,
                ServiceAccountSettings.REALM_NAME,
                AuthenticationField.API_KEY_ID_KEY,
                randomAlphaOfLength(20),
                AuthenticationField.API_KEY_NAME_KEY,
                randomAlphaOfLength(12),
                AuthenticationField.API_KEY_TYPE_KEY,
                ApiKey.Type.REST.value(),
                AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
                roleBytes,
                AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                limitedByRoleBytes
            )
        );

        final RoleReferenceIntersection roleReferenceIntersection = subject.getRoleReferenceIntersection(getAnonymousUser());
        final List<RoleReference> roleReferences = roleReferenceIntersection.getRoleReferences();
        assertThat(roleReferences, contains(isA(ApiKeyRoleReference.class), isA(ApiKeyRoleReference.class)));
        final ApiKeyRoleReference limitedByRoleReference = (ApiKeyRoleReference) roleReferences.get(1);
        assertThat(limitedByRoleReference.getRoleDescriptorsBytes(), equalTo(FLEET_SERVER_ROLE_DESCRIPTOR_BYTES_V_7_14));
    }

    private AnonymousUser getAnonymousUser() {
        final List<String> anonymousRoles = randomList(0, 2, () -> "role_anonymous_" + randomAlphaOfLength(8));
        return new AnonymousUser(Settings.builder().putList("xpack.security.authc.anonymous.roles", anonymousRoles).build());
    }
}
