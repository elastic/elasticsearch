/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class ChangePasswordRequestBuilderTests extends ESTestCase {

    public void testWithCleartextPassword() throws IOException {
        final Hasher hasher = getFastStoredHashAlgoForTests();
        final String json = """
            {
                "password": "superlongpassword"
            }""";
        ChangePasswordRequestBuilder builder = new ChangePasswordRequestBuilder(mock(Client.class));
        ChangePasswordRequest request = builder.source(new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, hasher)
            .request();
        assertThat(hasher.verify(new SecureString("superlongpassword".toCharArray()), request.passwordHash()), equalTo(true));
    }

    public void testWithHashedPassword() throws IOException {
        final Hasher hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString("superlongpassword".toCharArray()));
        final String json = """
            {
                "password_hash": "%s"
            }""".formatted(new String(hash));
        ChangePasswordRequestBuilder builder = new ChangePasswordRequestBuilder(mock(Client.class));
        ChangePasswordRequest request = builder.source(new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, hasher)
            .request();
        assertThat(request.passwordHash(), equalTo(hash));
    }

    public void testWithHashedPasswordWithWrongAlgo() {
        final Hasher systemHasher = getFastStoredHashAlgoForTests();
        Hasher userHasher = getFastStoredHashAlgoForTests();
        while (userHasher.name().equals(systemHasher.name())) {
            userHasher = getFastStoredHashAlgoForTests();
        }
        final char[] hash = userHasher.hash(new SecureString("superlongpassword".toCharArray()));
        final String json = """
            {"password_hash": "%s"}
            """.formatted(new String(hash));
        ChangePasswordRequestBuilder builder = new ChangePasswordRequestBuilder(mock(Client.class));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { builder.source(new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, systemHasher).request(); }
        );
        assertThat(e.getMessage(), containsString(userHasher.name()));
        assertThat(e.getMessage(), containsString(systemHasher.name()));
    }

    public void testWithHashedPasswordNotHash() {
        final Hasher systemHasher = getFastStoredHashAlgoForTests();
        final char[] hash = randomAlphaOfLength(20).toCharArray();
        final String json = """
            {
                "password_hash": "%s"
            }""".formatted(new String(hash));
        ChangePasswordRequestBuilder builder = new ChangePasswordRequestBuilder(mock(Client.class));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { builder.source(new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, systemHasher).request(); }
        );
        assertThat(e.getMessage(), containsString(Hasher.NOOP.name()));
        assertThat(e.getMessage(), containsString(systemHasher.name()));
    }

    public void testWithPasswordAndHash() throws IOException {
        final Hasher hasher = getFastStoredHashAlgoForTests();
        final String password = randomAlphaOfLength(14);
        final char[] hash = hasher.hash(new SecureString(password.toCharArray()));
        final LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        fields.put("password", password);
        fields.put("password_hash", new String(hash));
        BytesReference json = BytesReference.bytes(
            XContentBuilder.builder(XContentType.JSON.xContent()).map(shuffleMap(fields, Collections.emptySet()))
        );

        ChangePasswordRequestBuilder builder = new ChangePasswordRequestBuilder(mock(Client.class));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { builder.source(json, XContentType.JSON, hasher).request(); }
        );
        assertThat(e.getMessage(), containsString("password_hash has already been set"));

    }
}
