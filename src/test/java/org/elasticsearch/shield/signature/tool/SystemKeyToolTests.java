/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.signature.tool;

import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.signature.InternalSignatureService;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import static org.elasticsearch.shield.signature.tool.SystemKeyTool.Generate;
import static org.hamcrest.Matchers.*;
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
    }

    @Test
    public void testParse_NoArgs() throws Exception {
        CliTool.Command cmd = new SystemKeyTool().parse("generate", args(""));
        assertThat(cmd, instanceOf(Generate.class));
        Generate generate = (Generate) cmd;
        assertThat(generate.path, nullValue());
    }

    @Test
    public void testParse_FileArg() throws Exception {
        Path path = newTempFile().toPath();
        CliTool.Command cmd = new SystemKeyTool().parse("generate", new String[]{path.toAbsolutePath().toString()});
        assertThat(cmd, instanceOf(Generate.class));
        Generate generate = (Generate) cmd;
        assertThat(Files.isSameFile(generate.path, path), is(true));
    }

    @Test
    public void testGenerate() throws Exception {
        Path path = newTempFile().toPath();
        Generate generate = new Generate(terminal, path);
        CliTool.ExitStatus status = generate.execute(ImmutableSettings.EMPTY, env);
        assertThat(status, is(CliTool.ExitStatus.OK));
        byte[] bytes = Streams.copyToByteArray(path.toFile());
        assertThat(bytes.length, is(InternalSignatureService.KEY_SIZE / 8));
    }

    @Test
    public void testGenerate_PathInSettings() throws Exception {
        Path path = newTempFile().toPath();
        Settings settings = ImmutableSettings.builder()
                .put("shield.system_key.file", path.toAbsolutePath().toString())
                .build();
        Generate generate = new Generate(terminal, null);
        CliTool.ExitStatus status = generate.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.OK));
        byte[] bytes = Streams.copyToByteArray(path.toFile());
        assertThat(bytes.length, is(InternalSignatureService.KEY_SIZE / 8));
    }

    @Test
    public void testGenerate_DefaultPath() throws Exception {
        File config = newTempDir();
        File shieldConfig = new File(config, ShieldPlugin.NAME);
        shieldConfig.mkdirs();
        Path path = new File(shieldConfig, "system_key").toPath();
        when(env.configFile()).thenReturn(config);
        Generate generate = new Generate(terminal, null);
        CliTool.ExitStatus status = generate.execute(ImmutableSettings.EMPTY, env);
        assertThat(status, is(CliTool.ExitStatus.OK));
        byte[] bytes = Streams.copyToByteArray(path.toFile());
        assertThat(bytes.length, is(InternalSignatureService.KEY_SIZE / 8));
    }

    @Test
    public void testThatSystemKeyMayOnlyBeReadByOwner() throws Exception {
        File config = newTempDir();
        File shieldConfig = new File(config, ShieldPlugin.NAME);
        shieldConfig.mkdirs();
        Path path = new File(shieldConfig, "system_key").toPath();

        // no posix file permissions, nothing to test, done here
        boolean supportsPosixPermissions = Files.getFileStore(shieldConfig.toPath()).supportsFileAttributeView(PosixFileAttributeView.class);
        assumeTrue("Ignoring because posix file attributes are not supported", supportsPosixPermissions);

        when(env.configFile()).thenReturn(config);
        Generate generate = new Generate(terminal, null);
        CliTool.ExitStatus status = generate.execute(ImmutableSettings.EMPTY, env);
        assertThat(status, is(CliTool.ExitStatus.OK));

        Set<PosixFilePermission> posixFilePermissions = Files.getPosixFilePermissions(path);
        assertThat(posixFilePermissions, hasSize(2));
        assertThat(posixFilePermissions, containsInAnyOrder(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    }
}
