/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto.tool;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.crypto.InternalCryptoService;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

public class SystemKeyToolTests extends CommandTestCase {

    private FileSystem jimfs;
    private Settings.Builder settingsBuilder;
    private Path homeDir;

    private void initFileSystem(boolean needsPosix) throws Exception {
        String view = needsPosix ? "posix" : randomFrom("basic", "posix");
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews(view).build();
        jimfs = Jimfs.newFileSystem(conf);
        PathUtilsForTesting.installMock(jimfs);
        homeDir = jimfs.getPath("eshome");

        settingsBuilder = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), homeDir);
    }

    @Override
    protected Command newCommand() {
        return new SystemKeyTool(new Environment(settingsBuilder.build()));
    }

    public void testGenerate() throws Exception {
        initFileSystem(true);

        Path path = jimfs.getPath(randomAsciiOfLength(10)).resolve("key");
        Files.createDirectory(path.getParent());

        execute(path.toString());
        byte[] bytes = Files.readAllBytes(path);
        // TODO: maybe we should actually check the key is...i dunno...valid?
        assertEquals(InternalCryptoService.KEY_SIZE / 8, bytes.length);

        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_READ));
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_WRITE));
        assertEquals(perms.toString(), 2, perms.size());
    }

    public void testGeneratePathInSettings() throws Exception {
        initFileSystem(false);

        Path path = jimfs.getPath(randomAsciiOfLength(10)).resolve("key");
        Files.createDirectories(path.getParent());
        settingsBuilder.put(InternalCryptoService.FILE_SETTING.getKey(), path.toAbsolutePath().toString());
        execute();
        byte[] bytes = Files.readAllBytes(path);
        assertEquals(InternalCryptoService.KEY_SIZE / 8, bytes.length);
    }

    public void testGenerateDefaultPath() throws Exception {
        initFileSystem(false);
        Path keyPath = homeDir.resolve("config/x-pack/system_key");
        Files.createDirectories(keyPath.getParent());
        execute();
        byte[] bytes = Files.readAllBytes(keyPath);
        assertEquals(InternalCryptoService.KEY_SIZE / 8, bytes.length);
    }

    public void testThatSystemKeyMayOnlyBeReadByOwner() throws Exception {
        initFileSystem(true);

        Path path = jimfs.getPath(randomAsciiOfLength(10)).resolve("key");
        Files.createDirectories(path.getParent());

        execute(path.toString());
        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_READ));
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_WRITE));
        assertEquals(perms.toString(), 2, perms.size());
    }
}
