/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.ApiKeyTests;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.ingest.SetSecurityUserProcessor.Property;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

public class SetSecurityUserProcessorTests extends ESTestCase {

    private ThreadContext threadContext;
    private SecurityContext securityContext;

    @Before
    public void setupObjects() {
        threadContext = new ThreadContext(Settings.EMPTY);
        securityContext = new SecurityContext(Settings.EMPTY, threadContext);
    }

    @SuppressWarnings("unchecked")
    public void testProcessorWithData() throws Exception {
        final Authentication authentication = randomAuthentication();
        authentication.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        if (authentication.getUser().fullName().startsWith("Service account - ")) {
            assertThat(result, not(hasKey("roles")));
            assertThat(result, not(hasKey("email")));
        } else {
            assertThat(result.get("email"), equalTo(authentication.getUser().email()));
            if (authentication.getUser().roles().length == 0) {
                assertThat(result, not(hasKey("roles")));
            } else {
                assertThat(result.get("roles"), equalTo(Arrays.asList(authentication.getUser().roles())));
            }
        }
        if (authentication.getUser().metadata().isEmpty()) {
            assertThat(result, not(hasKey("metadata")));
        } else {
            assertThat(result.get("metadata"), equalTo(authentication.getUser().metadata()));
        }
        assertThat(result.get("username"), equalTo(authentication.getUser().principal()));
        assertThat(result.get("full_name"), equalTo(authentication.getUser().fullName()));
        assertThat(((Map<String, String>) result.get("realm")).get("name"), equalTo(authentication.getSourceRealm().getName()));
        assertThat(((Map<String, String>) result.get("realm")).get("type"), equalTo(authentication.getSourceRealm().getType()));
        assertThat(result.get("authentication_type"), equalTo(authentication.getAuthenticationType().toString()));
    }

    @SuppressWarnings("unchecked")
    public void testProcessorWithEmptyUserData() throws Exception {
        // test when user returns null for all values (need a mock, because a real user cannot have a null username)
        User user = Mockito.mock(User.class);
        Authentication authentication = Mockito.mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        final Authentication.RealmRef authByRealm = new Authentication.RealmRef("_name", "_type", "_node_name");
        when(authentication.getSourceRealm()).thenReturn(authByRealm);
        when(authentication.getAuthenticatedBy()).thenReturn(authByRealm);
        when(authentication.getAuthenticationType()).thenReturn(AuthenticationType.REALM);
        when(authentication.encode()).thenReturn(randomAlphaOfLength(24)); // don't care as long as it's not null
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        // Still holds data for realm and authentication type
        assertThat(result.size(), equalTo(2));
        assertThat(((Map) result.get("realm")).get("name"), equalTo("_name"));
        assertThat(((Map) result.get("realm")).get("type"), equalTo("_type"));
        assertThat(result.get("authentication_type"), equalTo("REALM"));
    }

    public void testNoCurrentUser() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.allOf(Property.class));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(),
            equalTo("There is no authenticated user - the [set_security_user] processor requires an authenticated user"));
    }

    public void testSecurityDisabled() throws Exception {
        Settings securityDisabledSettings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, securityDisabledSettings, "_field", EnumSet.allOf(Property.class));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("Security (authentication) is not enabled on this cluster, so there is no active user" +
            " - the [set_security_user] processor cannot be used without security"));
    }

    public void testUsernameProperties() throws Exception {
        final Authentication authentication = randomAuthentication();
        authentication.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.of(Property.USERNAME));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("username"), equalTo(authentication.getUser().principal()));
    }

    public void testRolesProperties() throws Exception {
        final Authentication authentication = randomAuthentication();
        authentication.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.of(Property.ROLES));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        if (authentication.getUser().roles().length == 0) {
            assertThat(result, not(hasKey("roles")));
        } else {
            assertThat(result.size(), equalTo(1));
            assertThat(result.get("roles"), equalTo(Arrays.asList(authentication.getUser().roles())));
        }
    }

    public void testFullNameProperties() throws Exception {
        final Authentication authentication = randomAuthentication();
        authentication.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor
            = new SetSecurityUserProcessor("_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.of(Property.FULL_NAME));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("full_name"), equalTo(authentication.getUser().fullName()));
    }

    public void testEmailProperties() throws Exception {
        final Authentication authentication = randomAuthentication();
        authentication.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.of(Property.EMAIL));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        if (authentication.getUser().email() != null) {
            assertThat(result.size(), equalTo(1));
            assertThat(result.get("email"), equalTo(authentication.getUser().email()));
        } else {
            assertThat(result, not(hasKey("email")));
        }
    }

    public void testMetadataProperties() throws Exception {
        final Authentication authentication = randomAuthentication();
        authentication.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.of(Property.METADATA));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        if (authentication.getUser().metadata().isEmpty()) {
            assertThat(result, not(hasKey("metadata")));
        } else {
            assertThat(result.size(), equalTo(1));
            assertThat(result.get("metadata"), equalTo(authentication.getUser().metadata()));
        }
    }

    public void testOverwriteExistingField() throws Exception {
        final Authentication authentication = randomAuthentication();
        authentication.writeToContext(threadContext);

        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.of(Property.USERNAME));

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        ingestDocument.setFieldValue("_field", "test");
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("username"), equalTo(authentication.getUser().principal()));

        ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        ingestDocument.setFieldValue("_field.other", "test");
        ingestDocument.setFieldValue("_field.username", "test");
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result2 = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result2.size(), equalTo(2));
        assertThat(result2.get("username"), equalTo(authentication.getUser().principal()));
        assertThat(result2.get("other"), equalTo("test"));
    }

    @SuppressWarnings("unchecked")
    public void testApiKeyPopulation() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12), null, null);
        Authentication.RealmRef realmRef = new Authentication.RealmRef(
            ApiKeyService.API_KEY_REALM_NAME, ApiKeyService.API_KEY_REALM_TYPE, "_node_name");

        final Map<String, Object> authMetadata = new HashMap<>(Map.of(
            ApiKeyService.API_KEY_ID_KEY, "api_key_id",
            ApiKeyService.API_KEY_NAME_KEY, "api_key_name",
            ApiKeyService.API_KEY_CREATOR_REALM_NAME, "creator_realm_name",
            ApiKeyService.API_KEY_CREATOR_REALM_TYPE, "creator_realm_type"
        ));
        final Map<String, Object> apiKeyMetadata = ApiKeyTests.randomMetadata();
        if (apiKeyMetadata != null) {
            authMetadata.put(ApiKeyService.API_KEY_METADATA_KEY, XContentTestUtils.convertToXContent(apiKeyMetadata, XContentType.JSON));
        }

        Authentication auth = new Authentication(user, realmRef, null, Version.CURRENT,
            AuthenticationType.API_KEY, authMetadata);
        auth.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(4));
        final Map<String, Object> apiKeyMap = (Map<String, Object>) result.get("api_key");
        assertThat(apiKeyMap.get("name"), equalTo("api_key_name"));
        assertThat(apiKeyMap.get("id"), equalTo("api_key_id"));
        if (apiKeyMetadata == null || apiKeyMetadata.isEmpty()) {
            assertNull(apiKeyMap.get("metadata"));
        } else {
            assertThat(apiKeyMap.get("metadata"), equalTo(apiKeyMetadata));
        }
        assertThat(((Map<String, String>) result.get("realm")).get("name"), equalTo("creator_realm_name"));
        assertThat(((Map<String, String>) result.get("realm")).get("type"), equalTo("creator_realm_type"));
        assertThat(result.get("authentication_type"), equalTo("API_KEY"));
    }

    @SuppressWarnings("unchecked")
    public void testWillNotOverwriteExistingApiKeyAndRealm() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12), null, null);
        Authentication.RealmRef realmRef = new Authentication.RealmRef(
            ApiKeyService.API_KEY_REALM_NAME, ApiKeyService.API_KEY_REALM_TYPE, "_node_name");

        final Map<String, Object> authMetadata = new HashMap<>(Map.of(
            ApiKeyService.API_KEY_ID_KEY, "api_key_id",
            ApiKeyService.API_KEY_NAME_KEY, "api_key_name",
            ApiKeyService.API_KEY_CREATOR_REALM_NAME, "creator_realm_name",
            ApiKeyService.API_KEY_CREATOR_REALM_TYPE, "creator_realm_type"
        ));
        final Map<String, Object> apiKeyMetadata = ApiKeyTests.randomMetadata();
        if (apiKeyMetadata != null) {
            authMetadata.put(ApiKeyService.API_KEY_METADATA_KEY, XContentTestUtils.convertToXContent(apiKeyMetadata, XContentType.JSON));
        }

        Authentication auth = new Authentication(user, realmRef, null, Version.CURRENT,
            AuthenticationType.API_KEY, authMetadata);
        auth.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(IngestDocument.deepCopyMap(Map.of(
            "_field", Map.of("api_key", Map.of("version", 42), "realm", Map.of("id", 7))
        )), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(4));
        assertThat(((Map<String, Integer>) result.get("api_key")).get("version"), equalTo(42));
        assertThat(((Map<String, Integer>) result.get("realm")).get("id"), equalTo(7));
    }

    @SuppressWarnings("unchecked")
    public void testWillSetRunAsRealmForNonApiKeyAuth() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12), null, null);
        Authentication.RealmRef authRealmRef = new Authentication.RealmRef(
            randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));
        Authentication.RealmRef lookedUpRealmRef = new Authentication.RealmRef(
            randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));

        Authentication auth = new Authentication(user, authRealmRef, lookedUpRealmRef, Version.CURRENT,
            randomFrom(AuthenticationType.REALM, AuthenticationType.TOKEN, AuthenticationType.INTERNAL),
            Collections.emptyMap());
        auth.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, Settings.EMPTY, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(3));
        assertThat(((Map<String, String>) result.get("realm")).get("name"), equalTo(lookedUpRealmRef.getName()));
        assertThat(((Map<String, String>) result.get("realm")).get("type"), equalTo(lookedUpRealmRef.getType()));
    }

    private User randomUser() {
        final User user = doRandomUser();
        if (false == user.fullName().startsWith("Service account - ") && randomBoolean()) {
            return new User(user, doRandomUser());
        } else {
            return user;
        }
    }

    private User doRandomUser() {
        if (randomIntBetween(0, 2) < 2) {
            return new User(randomAlphaOfLengthBetween(3, 8),
                randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)),
                randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(8, 20),
                randomFrom(Map.of(), Map.of("key", "value")), true);
        } else {
            final String principal = randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8);
            return new User(principal, Strings.EMPTY_ARRAY, "Service account - " + principal, null,
                randomFrom(Map.of(), Map.of("_elastic_service_account", true)), true);
        }
    }

    private Authentication randomAuthentication() {
        final User user = randomUser();
        if (user.fullName().startsWith("Service account - ")) {
            assert false == user.isRunAs() : "cannot run-as service account";
            final Authentication.RealmRef authBy =
                new Authentication.RealmRef("_service_account", "_service_account", randomAlphaOfLengthBetween(3, 8));
            final TokenInfo.TokenSource tokenSource = randomFrom(TokenInfo.TokenSource.values());
            return new Authentication(user, authBy, null, Version.CURRENT, AuthenticationType.TOKEN,
                Map.of("_token_name", ValidationTests.randomTokenName(), "_token_source", tokenSource.name().toLowerCase(Locale.ROOT)));
        } else {
            final Authentication.RealmRef lookupBy;
            final String nodeName = randomAlphaOfLengthBetween(3, 8);
            if (user.isRunAs()) {
                lookupBy = new Authentication.RealmRef(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8), nodeName);
            } else {
                lookupBy = null;
            }
            final Authentication.RealmRef authBy =
                new Authentication.RealmRef(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8), nodeName);
            final AuthenticationType authenticationType = user.isRunAs() ? AuthenticationType.REALM
                : randomFrom(AuthenticationType.REALM, AuthenticationType.INTERNAL, AuthenticationType.TOKEN, AuthenticationType.ANONYMOUS);
            final Map<String, Object> metadata = user.isRunAs() ? Map.of() : randomFrom(Map.of(), Map.of("foo", "bar"));
            return new Authentication(user, authBy, lookupBy, Version.CURRENT, authenticationType, metadata);
        }
    }
}
