/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.ingest.SetSecurityUserProcessor.Property;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SetSecurityUserProcessorTests extends ESTestCase {

    private ThreadContext threadContext;
    private SecurityContext securityContext;

    @Before
    public void setupObjects() {
        threadContext = new ThreadContext(Settings.EMPTY);
        securityContext = new SecurityContext(Settings.EMPTY, threadContext);
    }

    public void testProcessorWithData() throws Exception {
        User user = new User("_username", new String[] { "role1", "role2" }, "firstname lastname", "_email",
            Map.of("key", "value"), true);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        new Authentication(user, realmRef, null).writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", securityContext, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(5));
        assertThat(result.get("username"), equalTo("_username"));
        assertThat(result.get("roles"), equalTo(Arrays.asList("role1", "role2")));
        assertThat(result.get("full_name"), equalTo("firstname lastname"));
        assertThat(result.get("email"), equalTo("_email"));
        assertThat(((Map) result.get("metadata")).size(), equalTo(1));
        assertThat(((Map) result.get("metadata")).get("key"), equalTo("value"));
    }

    public void testProcessorWithEmptyUserData() throws Exception {
        // test when user returns null for all values (need a mock, because a real user cannot have a null username)
        User user = Mockito.mock(User.class);
        Authentication authentication = Mockito.mock(Authentication.class);
        Mockito.when(authentication.getUser()).thenReturn(user);
        Mockito.when(authentication.encode()).thenReturn(randomAlphaOfLength(24)); // don't care as long as it's not null
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", securityContext, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);
        Map result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(0));
    }

    public void testNoCurrentUser() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", securityContext, "_field", EnumSet.allOf(Property.class));
        IllegalStateException e = expectThrows(IllegalStateException.class,  () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("No user authenticated, only use this processor via authenticated user"));
    }

    public void testUsernameProperties() throws Exception {
        User user = new User("_username", null, null);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        new Authentication(user, realmRef, null).writeToContext(threadContext);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", securityContext, "_field", EnumSet.of(Property.USERNAME));
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
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", securityContext, "_field", EnumSet.of(Property.ROLES));
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
            = new SetSecurityUserProcessor("_tag", securityContext, "_field", EnumSet.of(Property.FULL_NAME));
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
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", securityContext, "_field", EnumSet.of(Property.EMAIL));
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
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", securityContext, "_field", EnumSet.of(Property.METADATA));
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

        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", securityContext, "_field", EnumSet.of(Property.USERNAME));

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

}
