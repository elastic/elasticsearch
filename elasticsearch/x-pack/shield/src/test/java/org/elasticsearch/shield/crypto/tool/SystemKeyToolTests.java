/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto.tool;

import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.Shield;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.shield.crypto.tool.SystemKeyTool.Generate;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class SystemKeyToolTests extends CliToolTestCase {
    private Terminal terminal;
    private Environment env;

    @Before
    public void init() throws Exception {
        terminal = mock(Terminal.class);
        env = mock(Environment.class);
        Path tmpDir = createTempDir();
        when(env.binFile()).thenReturn(tmpDir.resolve("bin"));
    }

    public void testParseNoArgs() throws Exception {
        CliTool.Command cmd = new SystemKeyTool().parse("generate", args(""));
        assertThat(cmd, instanceOf(Generate.class));
        Generate generate = (Generate) cmd;
        assertThat(generate.path, nullValue());
    }

    public void testParseFileArg() throws Exception {
        Path path = createTempFile();
        CliTool.Command cmd = new SystemKeyTool().parse("generate", new String[]{path.toAbsolutePath().toString()});
        assertThat(cmd, instanceOf(Generate.class));
        Generate generate = (Generate) cmd;

        // The test framework wraps paths so we can't compare path to path
        assertThat(generate.path.toString(), equalTo(path.toString()));
    }

    public void testGenerate() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path path = createTempFile();
        Generate generate = new Generate(terminal, path);
        CliTool.ExitStatus status = generate.execute(Settings.EMPTY, env);
        assertThat(status, is(CliTool.ExitStatus.OK));
        byte[] bytes = Files.readAllBytes(path);
        assertThat(bytes.length, is(InternalCryptoService.KEY_SIZE / 8));
    }

    public void testGeneratePathInSettings() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path path = createTempFile();
        Settings settings = Settings.builder()
                .put("shield.system_key.file", path.toAbsolutePath().toString())
                .build();
        Generate generate = new Generate(terminal, null);
        CliTool.ExitStatus status = generate.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.OK));
        byte[] bytes = Files.readAllBytes(path);
        assertThat(bytes.length, is(InternalCryptoService.KEY_SIZE / 8));
    }

    public void testGenerateDefaultPath() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path config = createTempDir();
        Path shieldConfig = config.resolve(Shield.NAME);
        Files.createDirectories(shieldConfig);
        Path path = shieldConfig.resolve("system_key");
        when(env.configFile()).thenReturn(config);
        Generate generate = new Generate(terminal, null);
        CliTool.ExitStatus status = generate.execute(Settings.EMPTY, env);
        assertThat(status, is(CliTool.ExitStatus.OK));
        byte[] bytes = Files.readAllBytes(path);
        assertThat(bytes.length, is(InternalCryptoService.KEY_SIZE / 8));
    }

    public void testThatSystemKeyMayOnlyBeReadByOwner() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path config = createTempDir();
        Path shieldConfig = config.resolve(Shield.NAME);
        Files.createDirectories(shieldConfig);
        Path path = shieldConfig.resolve("system_key");

        // no posix file permissions, nothing to test, done here
        boolean supportsPosixPermissions = Environment.getFileStore(shieldConfig).supportsFileAttributeView(PosixFileAttributeView.class);
        assumeTrue("Ignoring because posix file attributes are not supported", supportsPosixPermissions);

        when(env.configFile()).thenReturn(config);
        Generate generate = new Generate(terminal, null);
        CliTool.ExitStatus status = generate.execute(Settings.EMPTY, env);
        assertThat(status, is(CliTool.ExitStatus.OK));

        Set<PosixFilePermission> posixFilePermissions = Files.getPosixFilePermissions(path);
        assertThat(posixFilePermissions, hasSize(2));
        assertThat(posixFilePermissions, containsInAnyOrder(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    }
}
