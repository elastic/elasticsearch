/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.env;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PosixPermissionsResetter;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.HashSet;

public class NodeEnvironmentEvilTests extends ESTestCase {

    private static boolean isPosix;

    @BeforeClass
    public static void checkPosix() throws IOException {
        isPosix = Files.getFileAttributeView(createTempFile(), PosixFileAttributeView.class) != null;
    }

    public void testMissingWritePermission() throws IOException {
        assumeTrue("posix filesystem", isPosix);
        Path path = createTempDir();
        try (PosixPermissionsResetter attr = new PosixPermissionsResetter(path)) {
            attr.setPermissions(new HashSet<>(Arrays.asList(PosixFilePermission.OTHERS_READ, PosixFilePermission.GROUP_READ,
                PosixFilePermission.OWNER_READ)));
            Settings build = Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                    .put(Environment.PATH_DATA_SETTING.getKey(), path).build();
            IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
                new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
            });
            assertTrue(exception.getCause().getCause().getMessage(),
                exception.getCause().getCause().getMessage().startsWith(path.toString()));
        }
    }

    public void testMissingWritePermissionOnIndex() throws IOException {
        assumeTrue("posix filesystem", isPosix);
        Path path = createTempDir();
        Path fooIndex = path.resolve(NodeEnvironment.INDICES_FOLDER)
            .resolve("foo");
        Files.createDirectories(fooIndex);
        try (PosixPermissionsResetter attr = new PosixPermissionsResetter(fooIndex)) {
            attr.setPermissions(new HashSet<>(Arrays.asList(PosixFilePermission.OTHERS_READ, PosixFilePermission.GROUP_READ,
                PosixFilePermission.OWNER_READ)));
            Settings build = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                .put(Environment.PATH_DATA_SETTING.getKey(), path).build();
            IOException ioException = expectThrows(IOException.class, () -> {
                new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
            });
            assertTrue(ioException.getMessage(), ioException.getMessage().startsWith("failed to test writes in data directory"));
        }
    }

    public void testMissingWritePermissionOnShard() throws IOException {
        assumeTrue("posix filesystem", isPosix);
        Path path = createTempDir();
        Path fooIndex = path.resolve(NodeEnvironment.INDICES_FOLDER)
            .resolve("foo");
        Path fooShard = fooIndex.resolve("0");
        Path fooShardIndex = fooShard.resolve("index");
        Path fooShardTranslog = fooShard.resolve("translog");
        Path fooShardState = fooShard.resolve("_state");
        Path pick = randomFrom(fooShard, fooShardIndex, fooShardTranslog, fooShardState);
        Files.createDirectories(pick);
        try (PosixPermissionsResetter attr = new PosixPermissionsResetter(pick)) {
            attr.setPermissions(new HashSet<>(Arrays.asList(PosixFilePermission.OTHERS_READ, PosixFilePermission.GROUP_READ,
                PosixFilePermission.OWNER_READ)));
            Settings build = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                .put(Environment.PATH_DATA_SETTING.getKey(), path).build();
            IOException ioException = expectThrows(IOException.class, () -> {
                new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
            });
            assertTrue(ioException.getMessage(), ioException.getMessage().startsWith("failed to test writes in data directory"));
        }
    }
}
