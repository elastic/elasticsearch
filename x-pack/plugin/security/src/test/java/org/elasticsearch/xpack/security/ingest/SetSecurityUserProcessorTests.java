/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.ingest.SetSecurityUserProcessor.Property;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

public class SetSecurityUserProcessorTests extends ESTestCase {

    private ThreadContext threadContext;
    private SecurityContext securityContext;
    private XPackLicenseState licenseState;

    @Before
    public void setupObjects() {
        threadContext = new ThreadContext(Settings.EMPTY);
        securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        licenseState = Mockito.mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
    }

    public void testProcessorWithData() throws Exception {
        User user = new User("_username", new String[] { "role1", "role2" }, "firstname lastname", "_email",
            Map.of("key", "value"), true);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        new Authentication(user, realmRef, null, Version.CURRENT).writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, licenseState, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(7));
        assertThat(result.get("username"), equalTo("_username"));
        assertThat(result.get("roles"), equalTo(Arrays.asList("role1", "role2")));
        assertThat(result.get("full_name"), equalTo("firstname lastname"));
        assertThat(result.get("email"), equalTo("_email"));
        assertThat(((Map) result.get("metadata")).size(), equalTo(1));
        assertThat(((Map) result.get("metadata")).get("key"), equalTo("value"));
        assertThat(((Map) result.get("realm")).get("name"), equalTo("_name"));
        assertThat(((Map) result.get("realm")).get("type"), equalTo("_type"));
        assertThat(result.get("authentication_type"), equalTo("REALM"));
    }

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
            "_tag", null, securityContext, licenseState, "_field", EnumSet.allOf(Property.class));
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
            "_tag", null, securityContext, licenseState, "_field", EnumSet.allOf(Property.class));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(),
            equalTo("There is no authenticated user - the [set_security_user] processor requires an authenticated user"));
    }

    public void testSecurityDisabled() throws Exception {
        when(licenseState.isSecurityEnabled()).thenReturn(false);
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, licenseState, "_field", EnumSet.allOf(Property.class));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("Security (authentication) is not enabled on this cluster, so there is no active user" +
            " - the [set_security_user] processor cannot be used without security"));
    }

    public void testUsernameProperties() throws Exception {
        User user = new User("_username", null, null);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        new Authentication(user, realmRef, null).writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, licenseState, "_field", EnumSet.of(Property.USERNAME));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("username"), equalTo("_username"));
    }

    public void testRolesProperties() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12), "role1", "role2");
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        new Authentication(user, realmRef, null).writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, licenseState, "_field", EnumSet.of(Property.ROLES));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("roles"), equalTo(Arrays.asList("role1", "role2")));
    }

    public void testFullNameProperties() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12), null, "_full_name", null, Map.of(), true);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        new Authentication(user, realmRef, null).writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor
            = new SetSecurityUserProcessor("_tag", null, securityContext, licenseState, "_field", EnumSet.of(Property.FULL_NAME));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("full_name"), equalTo("_full_name"));
    }

    public void testEmailProperties() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12), null, null, "_email", Map.of(), true);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        new Authentication(user, realmRef, null).writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, licenseState, "_field", EnumSet.of(Property.EMAIL));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("email"), equalTo("_email"));
    }

    public void testMetadataProperties() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12), null, null, null, Map.of("key", "value"), true);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        new Authentication(user, realmRef, null).writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, licenseState, "_field", EnumSet.of(Property.METADATA));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(((Map) result.get("metadata")).size(), equalTo(1));
        assertThat(((Map) result.get("metadata")).get("key"), equalTo("value"));
    }

    public void testOverwriteExistingField() throws Exception {
        User user = new User("_username", null, null);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        new Authentication(user, realmRef, null).writeToContext(threadContext);

        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, licenseState, "_field", EnumSet.of(Property.USERNAME));

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        ingestDocument.setFieldValue("_field", "test");
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("username"), equalTo("_username"));

        ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        ingestDocument.setFieldValue("_field.other", "test");
        ingestDocument.setFieldValue("_field.username", "test");
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result2 = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result2.size(), equalTo(2));
        assertThat(result2.get("username"), equalTo("_username"));
        assertThat(result2.get("other"), equalTo("test"));
    }

    public void testApiKeyPopulation() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12), null, null);
        Authentication.RealmRef realmRef = new Authentication.RealmRef(
            ApiKeyService.API_KEY_REALM_NAME, ApiKeyService.API_KEY_REALM_TYPE, "_node_name");

        Authentication auth = new Authentication(user, realmRef, null, Version.CURRENT,
            AuthenticationType.API_KEY,
            Map.of(
                ApiKeyService.API_KEY_ID_KEY, "api_key_id",
                ApiKeyService.API_KEY_NAME_KEY, "api_key_name",
                ApiKeyService.API_KEY_CREATOR_REALM_NAME, "creator_realm_name",
                ApiKeyService.API_KEY_CREATOR_REALM_TYPE, "creator_realm_type"
            ));
        auth.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, licenseState, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(4));
        assertThat(((Map) result.get("api_key")).get("name"), equalTo("api_key_name"));
        assertThat(((Map) result.get("api_key")).get("id"), equalTo("api_key_id"));
        assertThat(((Map) result.get("realm")).get("name"), equalTo("creator_realm_name"));
        assertThat(((Map) result.get("realm")).get("type"), equalTo("creator_realm_type"));
        assertThat(result.get("authentication_type"), equalTo("API_KEY"));
    }

    public void testWillNotOverwriteExistingApiKeyAndRealm() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 12), null, null);
        Authentication.RealmRef realmRef = new Authentication.RealmRef(
            ApiKeyService.API_KEY_REALM_NAME, ApiKeyService.API_KEY_REALM_TYPE, "_node_name");

        Authentication auth = new Authentication(user, realmRef, null, Version.CURRENT,
            AuthenticationType.API_KEY,
            Map.of(
                ApiKeyService.API_KEY_ID_KEY, "api_key_id",
                ApiKeyService.API_KEY_NAME_KEY, "api_key_name",
                ApiKeyService.API_KEY_CREATOR_REALM_NAME, "creator_realm_name",
                ApiKeyService.API_KEY_CREATOR_REALM_TYPE, "creator_realm_type"
            ));
        auth.writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(IngestDocument.deepCopyMap(Map.of(
            "_field", Map.of("api_key", Map.of("version", 42), "realm", Map.of("id", 7))
        )), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor(
            "_tag", null, securityContext, licenseState, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(4));
        assertThat(((Map) result.get("api_key")).get("version"), equalTo(42));
        assertThat(((Map) result.get("realm")).get("id"), equalTo(7));
    }

    public void testWillSetRunAsRealmForNonApiAuth() throws Exception {
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
            "_tag", null, securityContext, licenseState, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(3));
        assertThat(((Map) result.get("realm")).get("name"), equalTo(lookedUpRealmRef.getName()));
        assertThat(((Map) result.get("realm")).get("type"), equalTo(lookedUpRealmRef.getType()));
    }

}
