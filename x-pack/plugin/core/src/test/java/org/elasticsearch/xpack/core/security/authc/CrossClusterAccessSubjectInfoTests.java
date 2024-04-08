/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class CrossClusterAccessSubjectInfoTests extends ESTestCase {

    public void testWriteReadContextRoundtrip() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection = randomRoleDescriptorsIntersection();
        final var expectedCrossClusterAccessSubjectInfo = new CrossClusterAccessSubjectInfo(
            AuthenticationTestHelper.builder().build(),
            expectedRoleDescriptorsIntersection
        );

        expectedCrossClusterAccessSubjectInfo.writeToContext(ctx);
        final CrossClusterAccessSubjectInfo actual = CrossClusterAccessSubjectInfo.readFromContext(ctx);

        assertThat(actual.getAuthentication(), equalTo(expectedCrossClusterAccessSubjectInfo.getAuthentication()));
        final List<Set<RoleDescriptor>> roleDescriptorsList = new ArrayList<>();
        for (CrossClusterAccessSubjectInfo.RoleDescriptorsBytes rdb : actual.getRoleDescriptorsBytesList()) {
            Set<RoleDescriptor> roleDescriptors = rdb.toRoleDescriptors();
            roleDescriptorsList.add(roleDescriptors);
        }
        final var actualRoleDescriptorsIntersection = new RoleDescriptorsIntersection(roleDescriptorsList);
        assertThat(actualRoleDescriptorsIntersection, equalTo(expectedRoleDescriptorsIntersection));
    }

    public void testRoleDescriptorsBytesToRoleDescriptors() throws IOException {
        final Set<RoleDescriptor> expectedRoleDescriptors = Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 3));
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.map(expectedRoleDescriptors.stream().collect(Collectors.toMap(RoleDescriptor::getName, Function.identity())));
        final Set<RoleDescriptor> actualRoleDescriptors = new CrossClusterAccessSubjectInfo.RoleDescriptorsBytes(
            BytesReference.bytes(builder)
        ).toRoleDescriptors();
        assertThat(actualRoleDescriptors, equalTo(expectedRoleDescriptors));
    }

    public void testThrowsOnMissingEntry() {
        var actual = expectThrows(
            IllegalArgumentException.class,
            () -> CrossClusterAccessSubjectInfo.readFromContext(new ThreadContext(Settings.EMPTY))
        );
        assertThat(
            actual.getMessage(),
            equalTo("cross cluster access header [" + CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY + "] is required")
        );
    }

    public void testCleanWithValidationForApiKeys() {
        final Map<String, Object> initialMetadata = newHashMapWithRandomMetadata();
        final AuthenticationTestHelper.AuthenticationTestBuilder builder = AuthenticationTestHelper.builder()
            .apiKey()
            .metadata(Map.copyOf(initialMetadata));
        // Also cover scenario where API-key runs as something else
        if (randomBoolean()) {
            builder.runAs();
        }
        final Authentication authentication = builder.build();
        final Map<String, Object> expectedMetadata = new HashMap<>();
        final Map<String, Object> authenticationMetadata = authentication.getAuthenticatingSubject().getMetadata();
        assertEntryPresentAndCopy(AuthenticationField.API_KEY_ID_KEY, authenticationMetadata, expectedMetadata);
        assertEntryPresentAndCopy(AuthenticationField.API_KEY_CREATOR_REALM_NAME, authenticationMetadata, expectedMetadata);
        assertEntryPresentAndCopy(AuthenticationField.API_KEY_CREATOR_REALM_TYPE, authenticationMetadata, expectedMetadata);
        assertEntryPresentAndCopy(AuthenticationField.API_KEY_NAME_KEY, authenticationMetadata, expectedMetadata);

        final Map<String, Object> actualMetadata = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(authentication)
            .cleanAndValidate()
            .getAuthentication()
            .getAuthenticatingSubject()
            .getMetadata();

        assertThat(actualMetadata, equalTo(expectedMetadata));
    }

    public void testCleanWithValidationForServiceAccounts() {
        final Map<String, Object> initialMetadata = newHashMapWithRandomMetadata();
        final AuthenticationTestHelper.AuthenticationTestBuilder builder = AuthenticationTestHelper.builder()
            .serviceAccount()
            .metadata(Map.copyOf(initialMetadata));
        final Authentication authentication = builder.build();
        final Map<String, Object> expectedMetadata = new HashMap<>();
        final Map<String, Object> authenticationMetadata = authentication.getAuthenticatingSubject().getMetadata();
        assertEntryPresentAndCopy(ServiceAccountSettings.TOKEN_NAME_FIELD, authenticationMetadata, expectedMetadata);
        assertEntryPresentAndCopy(ServiceAccountSettings.TOKEN_SOURCE_FIELD, authenticationMetadata, expectedMetadata);

        final Map<String, Object> actualMetadata = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(authentication)
            .cleanAndValidate()
            .getAuthentication()
            .getAuthenticatingSubject()
            .getMetadata();

        assertThat(actualMetadata, equalTo(expectedMetadata));
    }

    public void testCleanWithValidationWhenMetadataShouldBeEmpty() throws IOException {
        final Map<String, Object> metadata = newHashMapWithRandomMetadata();
        final Authentication authentication = randomRealmAuthenticationWithMetadata(metadata);
        assertThat(authentication.getAuthenticatingSubject().getMetadata(), equalTo(metadata));

        final Map<String, Object> actualMetadata = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(authentication)
            .cleanAndValidate()
            .getAuthentication()
            .getAuthenticatingSubject()
            .getMetadata();

        assertThat(actualMetadata, is(anEmptyMap()));
    }

    private Map<String, Object> newHashMapWithRandomMetadata() {
        final Map<String, Object> randomMetadata = new HashMap<>();
        if (randomBoolean()) {
            randomMetadata.put(ESTestCase.randomAlphaOfLength(42), ESTestCase.randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            randomMetadata.put(AuthenticationField.API_KEY_ID_KEY, ESTestCase.randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            randomMetadata.put(AuthenticationField.API_KEY_NAME_KEY, ESTestCase.randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            randomMetadata.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, new BytesArray("{}"));
        }
        if (randomBoolean()) {
            randomMetadata.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, new BytesArray("{}"));
        }
        if (randomBoolean()) {
            randomMetadata.put(
                AuthenticationField.API_KEY_METADATA_KEY,
                Map.of(ESTestCase.randomAlphaOfLength(5), ESTestCase.randomAlphaOfLength(42))
            );
        }
        if (randomBoolean()) {
            randomMetadata.put(ServiceAccountSettings.TOKEN_NAME_FIELD, ESTestCase.randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            randomMetadata.put(
                ServiceAccountSettings.TOKEN_SOURCE_FIELD,
                ESTestCase.randomFrom(TokenInfo.TokenSource.values()).name().toLowerCase(Locale.ROOT)
            );
        }
        return randomMetadata;
    }

    private void assertEntryPresentAndCopy(String key, Map<String, Object> sourceMap, Map<String, Object> targetMap) {
        assertThat(sourceMap, hasKey(key));
        targetMap.put(key, sourceMap.get(key));
    }

    public static RoleDescriptorsIntersection randomRoleDescriptorsIntersection() {
        return new RoleDescriptorsIntersection(randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1))));
    }

    private static Authentication randomRealmAuthenticationWithMetadata(Map<String, Object> metadata) throws IOException {
        final Authentication authentication = AuthenticationTestHelper.builder().realm().build(false);
        final Subject authenticatingSubject = authentication.getAuthenticatingSubject();
        final var authenticatingSubjectWithMetadata = new Subject(
            authenticatingSubject.getUser(),
            authenticatingSubject.getRealm(),
            authenticatingSubject.getTransportVersion(),
            metadata
        );
        // Hack that allows us to set metadata on a realm authentication
        return AuthenticationContextSerializer.decode(
            Authentication.doEncode(
                authenticatingSubjectWithMetadata,
                authenticatingSubjectWithMetadata,
                Authentication.AuthenticationType.REALM
            )
        );
    }
}
