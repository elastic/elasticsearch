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
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.ingest.SetSecurityUserProcessor.Property;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SetSecurityUserProcessorTests extends ESTestCase {

    public void testProcessor() throws Exception {
        User user = new User("_username", new String[]{"role1", "role2"}, "firstname lastname", "_email",
                Map.of("key", "value"), true);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, new Authentication(user, realmRef, null));

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", threadContext, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(5));
        assertThat(result.get("username"), equalTo("_username"));
        assertThat(((List) result.get("roles")).size(), equalTo(2));
        assertThat(((List) result.get("roles")).get(0), equalTo("role1"));
        assertThat(((List) result.get("roles")).get(1), equalTo("role2"));
        assertThat(result.get("full_name"), equalTo("firstname lastname"));
        assertThat(result.get("email"), equalTo("_email"));
        assertThat(((Map) result.get("metadata")).size(), equalTo(1));
        assertThat(((Map) result.get("metadata")).get("key"), equalTo("value"));

        // test when user holds no data:
        threadContext = new ThreadContext(Settings.EMPTY);
        user = new User(null, null, null);
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, new Authentication(user, realmRef, null));
        ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        processor = new SetSecurityUserProcessor("_tag", threadContext, "_field", EnumSet.allOf(Property.class));
        processor.execute(ingestDocument);
        result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(0));
    }

    public void testNoCurrentUser() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", threadContext, "_field", EnumSet.allOf(Property.class));
        IllegalStateException e = expectThrows(IllegalStateException.class,  () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("No user authenticated, only use this processor via authenticated user"));
    }

    public void testUsernameProperties() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User("_username", null, null);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, new Authentication(user, realmRef, null));

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", threadContext, "_field", EnumSet.of(Property.USERNAME));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("username"), equalTo("_username"));
    }

    public void testRolesProperties() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User(null, "role1", "role2");
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, new Authentication(user, realmRef, null));

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", threadContext, "_field", EnumSet.of(Property.ROLES));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(((List) result.get("roles")).size(), equalTo(2));
        assertThat(((List) result.get("roles")).get(0), equalTo("role1"));
        assertThat(((List) result.get("roles")).get(1), equalTo("role2"));
    }

    public void testFullNameProperties() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User(null, null, "_full_name", null, Map.of(), true);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, new Authentication(user, realmRef, null));

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", threadContext, "_field", EnumSet.of(Property.FULL_NAME));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("full_name"), equalTo("_full_name"));
    }

    public void testEmailProperties() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User(null, null, null, "_email", Map.of(), true);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, new Authentication(user, realmRef, null));

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", threadContext, "_field", EnumSet.of(Property.EMAIL));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("email"), equalTo("_email"));
    }

    public void testMetadataProperties() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User(null, null, null, null, Map.of("key", "value"), true);
        Authentication.RealmRef realmRef = new Authentication.RealmRef("_name", "_type", "_node_name");
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, new Authentication(user, realmRef, null));

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        SetSecurityUserProcessor processor = new SetSecurityUserProcessor("_tag", threadContext, "_field", EnumSet.of(Property.METADATA));
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = ingestDocument.getFieldValue("_field", Map.class);
        assertThat(result.size(), equalTo(1));
        assertThat(((Map) result.get("metadata")).size(), equalTo(1));
        assertThat(((Map) result.get("metadata")).get("key"), equalTo("value"));
    }

}
