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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Writes file realm users directly into the {@code users} and {@code users_roles} files.
 * <p>
 * Every {@code elasticsearch-users useradd} call starts a JVM, which costs seconds of CPU on a busy agent, so a cluster
 * with dozens of users can spend most of a suite timeout before the node starts. Users that share a password can reuse
 * the hash that {@code useradd} wrote for the first of them, which keeps the hash in the format the node is configured
 * to verify (e.g. PBKDF2 under FIPS).
 * <p>
 * Hashes are cached for the life of the test JVM, so later nodes and later test classes in the same Gradle fork reuse them
 * instead of starting another {@code useradd} JVM for every node of every cluster.
 */
final class FileRealmUsers {

    private static final Map<HashKey, String> PASSWORD_HASHES = new ConcurrentHashMap<>();

    /**
     * Identifies a hash that any node can verify: the same password, hashed by the same distribution version with the same
     * hashing settings. The FIPS provider is configured for the whole test JVM, so it is not part of the key.
     *
     * @param hashingAlgorithm the node's {@code xpack.security.authc.password_hashing.algorithm}, or null if unset
     * @param fipsMode the node's {@code xpack.security.fips_mode.enabled}, or null if unset
     */
    record HashKey(Version version, String password, String hashingAlgorithm, String fipsMode) {}

    private FileRealmUsers() {}

    /**
     * Returns a hash that {@code useradd} previously wrote for {@code key} in this JVM, or null if there is none yet.
     */
    static String cachedPasswordHash(HashKey key) {
        return PASSWORD_HASHES.get(key);
    }

    /**
     * Remembers a hash written by {@code useradd} so other nodes can reuse it. The first hash stored for a key wins, so
     * nodes that hash the same password concurrently all end up reusing one value.
     */
    static void cachePasswordHash(HashKey key, String passwordHash) {
        PASSWORD_HASHES.putIfAbsent(key, passwordHash);
    }

    /**
     * Returns the password hash stored for {@code username} in the {@code users} file in {@code configDir}.
     */
    static String readPasswordHash(Path configDir, String username) {
        Path usersFile = configDir.resolve("users");
        String prefix = username + ":";
        try {
            return Files.readAllLines(usersFile, StandardCharsets.UTF_8)
                .stream()
                .filter(line -> line.startsWith(prefix))
                .map(line -> line.substring(prefix.length()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("User [" + username + "] not found in [" + usersFile + "]"));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read users file " + usersFile, e);
        }
    }

    /**
     * Adds {@code username} with an already computed password hash and the given comma-separated roles, splitting them
     * the same way {@code useradd} does. The file realm merges repeated role lines in {@code users_roles}, so each role is
     * appended as its own line.
     * <p>
     * Unlike {@code useradd}, this does not validate the username or roles. The node applies the same checks when it loads
     * the files, so an invalid or reserved name is skipped with an error in the node log and requests as that user fail with
     * 401 instead of setup failing.
     */
    static void addUser(Path configDir, String username, String passwordHash, String roles) {
        append(configDir.resolve("users"), username + ":" + passwordHash + "\n");
        if (roles.isEmpty() == false) {
            append(
                configDir.resolve("users_roles"),
                Arrays.stream(roles.split(",")).map(role -> role + ":" + username + "\n").collect(Collectors.joining())
            );
        }
    }

    private static void append(Path file, String content) {
        try {
            Files.writeString(file, content, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to append to " + file, e);
        }
    }
}
