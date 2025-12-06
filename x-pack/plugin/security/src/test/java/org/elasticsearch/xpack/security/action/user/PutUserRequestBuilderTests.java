/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;

import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class PutUserRequestBuilderTests extends ESTestCase {

    public void testNullValuesForEmailAndFullName() throws IOException {
        final String json = """
            {
                "roles": [
                  "kibana4"
                ],
                "full_name": null,
                "email": null,
                "metadata": {}
            }""";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, Hasher.BCRYPT);

        PutUserRequest request = builder.request();
        assertThat(request.username(), is("kibana4"));
        assertThat(request.roles(), arrayContaining("kibana4"));
        assertThat(request.fullName(), nullValue());
        assertThat(request.email(), nullValue());
        assertThat(request.metadata().isEmpty(), is(true));
        assertTrue(request.enabled());
    }

    public void testMissingEmailFullName() throws Exception {
        final String json = """
            {
                "roles": [
                  "kibana4"
                ],
                "metadata": {}
            }""";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, Hasher.BCRYPT);

        PutUserRequest request = builder.request();
        assertThat(request.username(), is("kibana4"));
        assertThat(request.roles(), arrayContaining("kibana4"));
        assertThat(request.fullName(), nullValue());
        assertThat(request.email(), nullValue());
        assertThat(request.metadata().isEmpty(), is(true));
    }

    public void testWithFullNameAndEmail() throws IOException {
        final String json = """
            {
                "roles": [
                  "kibana4"
                ],
                "full_name": "Kibana User",
                "email": "kibana@elastic.co",
                "metadata": {}
            }""";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, Hasher.BCRYPT);

        PutUserRequest request = builder.request();
        assertThat(request.username(), is("kibana4"));
        assertThat(request.roles(), arrayContaining("kibana4"));
        assertThat(request.fullName(), is("Kibana User"));
        assertThat(request.email(), is("kibana@elastic.co"));
        assertThat(request.metadata().isEmpty(), is(true));
    }

    public void testInvalidFullname() throws IOException {
        final String json = """
            {
                "roles": [
                  "kibana4"
                ],
                "full_name": [ "Kibana User" ],
                "email": "kibana@elastic.co",
                "metadata": {}
            }""";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, Hasher.BCRYPT)
        );
        assertThat(e.getMessage(), containsString("expected field [full_name] to be of type string"));
    }

    public void testInvalidEmail() throws IOException {
        final String json = """
            {
                "roles": [
                  "kibana4"
                ],
                "full_name": "Kibana User",
                "email": [ "kibana@elastic.co" ],
                "metadata": {}
            }""";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, Hasher.BCRYPT)
        );
        assertThat(e.getMessage(), containsString("expected field [email] to be of type string"));
    }

    public void testWithEnabled() throws IOException {
        final String json = """
            {
                "roles": [
                  "kibana4"
                ],
                "full_name": "Kibana User",
                "email": "kibana@elastic.co",
                "metadata": {}
            ,    "enabled": false
            }""";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        PutUserRequest request = builder.source(
            "kibana4",
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON,
            Hasher.BCRYPT
        ).request();
        assertFalse(request.enabled());
    }

    public void testWithValidPasswordHash() throws IOException {
        final Hasher hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString("secretpassword".toCharArray()));
        final String json = Strings.format("""
            {
              "password_hash": "%s",
              "roles": []
            }""", new String(hash));

        PutUserRequestBuilder requestBuilder = new PutUserRequestBuilder(mock(Client.class));
        PutUserRequest request = requestBuilder.source(
            "hash_user",
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON,
            hasher
        ).request();
        assertThat(request.passwordHash(), equalTo(hash));
        assertThat(request.username(), equalTo("hash_user"));
    }

    public void testWithDifferentPasswordHashingAlgorithm() throws IOException {
        final Hasher systemHasher = getFastStoredHashAlgoForTests();
        Hasher userHasher = getFastStoredHashAlgoForTests();
        while (userHasher.name().equals(systemHasher.name())) {
            userHasher = getFastStoredHashAlgoForTests();
        }
        final char[] hash = userHasher.hash(new SecureString("secretpassword".toCharArray()));
        final String json = Strings.format("""
            {
              "password_hash": "%s",
              "roles": []
            }""", new String(hash));

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        PutUserRequest request = builder.source(
            "hash_user",
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON,
            systemHasher
        ).request();
        assertThat(request.passwordHash(), equalTo(hash));
    }

    public void testWithPasswordHashThatsNotReallyAHash() throws IOException {
        final Hasher systemHasher = Hasher.valueOf(randomFrom(Hasher.getAvailableAlgoStoredPasswordHash()).toUpperCase(Locale.ROOT));
        final char[] hash = randomAlphaOfLengthBetween(14, 20).toCharArray();
        final String json = Strings.format("""
            {
              "password_hash": "%s",
              "roles": []
            }""", new String(hash));

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            builder.source("hash_user", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, systemHasher).request();
        });
        assertThat(
            ex.getMessage(),
            containsString("The provided password hash is not a hash or it could not be resolved to a supported hash algorithm.")
        );
    }

    public void testWithBothPasswordAndHash() throws IOException {
        final Hasher hasher = getFastStoredHashAlgoForTests();
        final String password = randomAlphaOfLength(14);
        final char[] hash = hasher.hash(new SecureString(password.toCharArray()));
        final LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        fields.put("password", password);
        fields.put("password_hash", new String(hash));
        fields.put("roles", Collections.emptyList());
        BytesReference json = BytesReference.bytes(
            XContentBuilder.builder(XContentType.JSON.xContent()).map(shuffleMap(fields, Collections.emptySet()))
        );

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        final IllegalArgumentException ex = expectThrows(ValidationException.class, () -> {
            builder.source("hash_user", json, XContentType.JSON, hasher).request();
        });
        assertThat(ex.getMessage(), containsString("password_hash has already been set"));
    }
}
