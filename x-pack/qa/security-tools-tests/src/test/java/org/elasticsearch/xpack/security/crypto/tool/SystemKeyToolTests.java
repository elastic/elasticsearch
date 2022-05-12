/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto.tool;

import joptsimple.OptionSet;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.junit.After;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

public class SystemKeyToolTests extends CommandTestCase {

    private FileSystem jimfs;
    private Path homeDir;

    private void initFileSystem(boolean needsPosix) throws Exception {
        String view = needsPosix ? "posix" : randomFrom("basic", "posix");
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews(view).build();
        jimfs = Jimfs.newFileSystem(conf);
        PathUtilsForTesting.installMock(jimfs);
        homeDir = jimfs.getPath("eshome");
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.close(jimfs);
        super.tearDown();
    }

    @Override
    protected Command newCommand() {
        return new SystemKeyTool() {
            @Override
            protected Environment createEnv(OptionSet options, ProcessInfo processInfo) {
                // it would be better to mock the system properties here, but our generated file permissions
                // do not play nice with jimfs...
                Settings settings = Settings.builder().put("path.home", homeDir.toString()).build();
                return TestEnvironment.newEnvironment(settings);
            }
        };
    }

    public void testGenerate() throws Exception {
        initFileSystem(true);

        Path path = jimfs.getPath(randomAlphaOfLength(10)).resolve("key");
        Files.createDirectory(path.getParent());

        execute(path.toString());
        byte[] bytes = Files.readAllBytes(path);
        // TODO: maybe we should actually check the key is...i dunno...valid?
        assertEquals(SystemKeyTool.KEY_SIZE / 8, bytes.length);

        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_READ));
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_WRITE));
        assertEquals(perms.toString(), 2, perms.size());
    }

    public void testGeneratePathInSettings() throws Exception {
        initFileSystem(false);

        Path xpackConf = homeDir.resolve("config");
        Files.createDirectories(xpackConf);
        execute();
        byte[] bytes = Files.readAllBytes(xpackConf.resolve("system_key"));
        assertEquals(SystemKeyTool.KEY_SIZE / 8, bytes.length);
    }

    public void testGenerateDefaultPath() throws Exception {
        initFileSystem(false);
        Path keyPath = homeDir.resolve("config/system_key");
        Files.createDirectories(keyPath.getParent());
        execute();
        byte[] bytes = Files.readAllBytes(keyPath);
        assertEquals(SystemKeyTool.KEY_SIZE / 8, bytes.length);
    }

    public void testThatSystemKeyMayOnlyBeReadByOwner() throws Exception {
        initFileSystem(true);

        Path path = jimfs.getPath(randomAlphaOfLength(10)).resolve("key");
        Files.createDirectories(path.getParent());

        execute(path.toString());
        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_READ));
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_WRITE));
        assertEquals(perms.toString(), 2, perms.size());
    }

}
