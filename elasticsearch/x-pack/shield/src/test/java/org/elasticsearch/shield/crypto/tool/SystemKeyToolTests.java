/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto.tool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.Before;

public class SystemKeyToolTests extends ESTestCase {
    private Terminal terminal;
    private Environment env;

    @Before
    public void init() throws Exception {
        terminal = new CliToolTestCase.CaptureOutputTerminal();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        env = new Environment(settings);
    }

    public void testGenerate() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path path = createTempDir().resolve("key");
        new SystemKeyTool(env).execute(terminal, path);
        byte[] bytes = Files.readAllBytes(path);
        // TODO: maybe we should actually check the key is...i dunno...valid?
        assertEquals(InternalCryptoService.KEY_SIZE / 8, bytes.length);

        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_READ));
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_WRITE));
        assertEquals(perms.toString(), 2, perms.size());
    }

    public void testGeneratePathInSettings() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path path = createTempDir().resolve("key");
        Settings settings = Settings.builder().put(env.settings())
            .put("shield.system_key.file", path.toAbsolutePath().toString()).build();
        env = new Environment(settings);
        new SystemKeyTool(env).execute(terminal, (Path) null);
        byte[] bytes = Files.readAllBytes(path);
        assertEquals(InternalCryptoService.KEY_SIZE / 8, bytes.length);
    }

    public void testGenerateDefaultPath() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path keyPath = XPackPlugin.resolveConfigFile(env, "system_key");
        Files.createDirectories(keyPath.getParent());
        new SystemKeyTool(env).execute(terminal, (Path) null);
        byte[] bytes = Files.readAllBytes(keyPath);
        assertEquals(InternalCryptoService.KEY_SIZE / 8, bytes.length);
    }

    public void testThatSystemKeyMayOnlyBeReadByOwner() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path path = createTempDir().resolve("key");
        boolean isPosix = Files.getFileAttributeView(path.getParent(), PosixFileAttributeView.class) != null;
        assumeTrue("posix filesystem", isPosix);

        new SystemKeyTool(env).execute(terminal, path);
        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_READ));
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_WRITE));
        assertEquals(perms.toString(), 2, perms.size());
    }
}
