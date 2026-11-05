/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.util.Version;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;

public class FileRealmUsersTests {

    private static final String PBKDF2_HASH = "{PBKDF2_STRETCH}10000$c2FsdA==$aGFzaA==";

    @Test
    public void testReusesHashWrittenByUseradd() throws IOException {
        // given the files as written by `elasticsearch-users useradd first -r superuser`
        Path configDir = Files.createTempDirectory("config");
        Files.writeString(configDir.resolve("users"), "first:" + PBKDF2_HASH + "\n");
        Files.writeString(configDir.resolve("users_roles"), "superuser:first\n");

        // when
        String hash = FileRealmUsers.readPasswordHash(configDir, "first");
        FileRealmUsers.addUser(configDir, "second", hash, "role_a,role_b");

        // then
        assertThat(hash, is(PBKDF2_HASH));
        assertThat(
            Files.readAllLines(configDir.resolve("users"), StandardCharsets.UTF_8),
            is(List.of("first:" + PBKDF2_HASH, "second:" + PBKDF2_HASH))
        );
        assertThat(
            Files.readAllLines(configDir.resolve("users_roles"), StandardCharsets.UTF_8),
            is(List.of("superuser:first", "role_a:second", "role_b:second"))
        );
    }

    @Test
    public void testNoRolesWritesNoRolesLine() throws IOException {
        Path configDir = Files.createTempDirectory("config");
        Files.writeString(configDir.resolve("users_roles"), "superuser:first\n");

        FileRealmUsers.addUser(configDir, "second", PBKDF2_HASH, "");

        assertThat(Files.readAllLines(configDir.resolve("users"), StandardCharsets.UTF_8), is(List.of("second:" + PBKDF2_HASH)));
        assertThat(Files.readAllLines(configDir.resolve("users_roles"), StandardCharsets.UTF_8), is(List.of("superuser:first")));
    }

    @Test
    public void testDoesNotMatchUsernamePrefix() throws IOException {
        Path configDir = Files.createTempDirectory("config");
        Files.writeString(configDir.resolve("users"), "user_long:" + PBKDF2_HASH + "\n");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> FileRealmUsers.readPasswordHash(configDir, "user"));

        assertThat(e.getMessage(), containsString("User [user] not found"));
    }

    @Test
    public void testCachedHashIsReusedOnlyForTheSameKey() {
        // a unique password keeps this test independent of hashes other tests in this JVM may have cached
        String password = "password-" + UUID.randomUUID();
        FileRealmUsers.HashKey key = new FileRealmUsers.HashKey(Version.fromString("9.1.0"), password, null, null);

        assertThat(FileRealmUsers.cachedPasswordHash(key), is(nullValue()));
        FileRealmUsers.cachePasswordHash(key, PBKDF2_HASH);

        assertThat(
            FileRealmUsers.cachedPasswordHash(new FileRealmUsers.HashKey(Version.fromString("9.1.0"), password, null, null)),
            is(PBKDF2_HASH)
        );
        assertThat(
            FileRealmUsers.cachedPasswordHash(new FileRealmUsers.HashKey(Version.fromString("8.19.0"), password, null, null)),
            is(nullValue())
        );
        assertThat(
            FileRealmUsers.cachedPasswordHash(new FileRealmUsers.HashKey(Version.fromString("9.1.0"), password, "bcrypt", null)),
            is(nullValue())
        );
        assertThat(
            FileRealmUsers.cachedPasswordHash(new FileRealmUsers.HashKey(Version.fromString("9.1.0"), password, null, "true")),
            is(nullValue())
        );
        assertThat(
            FileRealmUsers.cachedPasswordHash(new FileRealmUsers.HashKey(Version.fromString("9.1.0"), password + "x", null, null)),
            is(nullValue())
        );
    }

    @Test
    public void testFirstCachedHashWins() {
        FileRealmUsers.HashKey key = new FileRealmUsers.HashKey(Version.fromString("9.1.0"), "password-" + UUID.randomUUID(), null, null);

        FileRealmUsers.cachePasswordHash(key, PBKDF2_HASH);
        FileRealmUsers.cachePasswordHash(key, "{PBKDF2_STRETCH}10000$b3RoZXI=$b3RoZXI=");

        assertThat(FileRealmUsers.cachedPasswordHash(key), is(PBKDF2_HASH));
    }
}
