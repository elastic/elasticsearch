/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.env;

import org.elasticsearch.common.io.PathUtils;
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
        final String[] tempPaths = tmpPaths();
        Path path = PathUtils.get(randomFrom(tempPaths));
        try (PosixPermissionsResetter attr = new PosixPermissionsResetter(path)) {
            attr.setPermissions(new HashSet<>(Arrays.asList(PosixFilePermission.OTHERS_READ, PosixFilePermission.GROUP_READ,
                PosixFilePermission.OWNER_READ)));
            Settings build = Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                    .putList(Environment.PATH_DATA_SETTING.getKey(), tempPaths).build();
            IOException ioException = expectThrows(IOException.class, () -> {
                new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
            });
            assertTrue(ioException.getMessage(), ioException.getMessage().startsWith(path.toString()));
        }
    }

    public void testMissingWritePermissionOnIndex() throws IOException {
        assumeTrue("posix filesystem", isPosix);
        final String[] tempPaths = tmpPaths();
        Path path = PathUtils.get(randomFrom(tempPaths));
        Path fooIndex = path.resolve("nodes").resolve("0").resolve(NodeEnvironment.INDICES_FOLDER)
            .resolve("foo");
        Files.createDirectories(fooIndex);
        try (PosixPermissionsResetter attr = new PosixPermissionsResetter(fooIndex)) {
            attr.setPermissions(new HashSet<>(Arrays.asList(PosixFilePermission.OTHERS_READ, PosixFilePermission.GROUP_READ,
                PosixFilePermission.OWNER_READ)));
            Settings build = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                .putList(Environment.PATH_DATA_SETTING.getKey(), tempPaths).build();
            IOException ioException = expectThrows(IOException.class, () -> {
                new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
            });
            assertTrue(ioException.getMessage(), ioException.getMessage().startsWith("failed to test writes in data directory"));
        }
    }

    public void testMissingWritePermissionOnShard() throws IOException {
        assumeTrue("posix filesystem", isPosix);
        final String[] tempPaths = tmpPaths();
        Path path = PathUtils.get(randomFrom(tempPaths));
        Path fooIndex = path.resolve("nodes").resolve("0").resolve(NodeEnvironment.INDICES_FOLDER)
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
                .putList(Environment.PATH_DATA_SETTING.getKey(), tempPaths).build();
            IOException ioException = expectThrows(IOException.class, () -> {
                new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
            });
            assertTrue(ioException.getMessage(), ioException.getMessage().startsWith("failed to test writes in data directory"));
        }
    }
}
