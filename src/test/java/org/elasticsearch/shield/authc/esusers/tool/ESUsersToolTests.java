/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers.tool;

import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.support.Hasher;
import org.junit.Test;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class ESUsersToolTests extends CliToolTestCase {

    @Test
    public void testUseradd_Parse_AllOptions() throws Exception {
        ESUsersTool tool = new ESUsersTool();
        CliTool.Command command = tool.parse("useradd", args("username -p changeme -r r1,r2,r3"));
        assertThat(command, instanceOf(ESUsersTool.Useradd.class));
        ESUsersTool.Useradd cmd = (ESUsersTool.Useradd) command;
        assertThat(cmd.username, equalTo("username"));
        assertThat(new String(cmd.passwd), equalTo("changeme"));
        assertThat(cmd.roles, notNullValue());
        assertThat(cmd.roles, arrayContaining("r1", "r2", "r3"));
    }

    @Test
    public void testUseradd_Parse_NoUsername() throws Exception {
        ESUsersTool tool = new ESUsersTool();
        CliTool.Command command = tool.parse("useradd", args("-p test123"));
        assertThat(command, instanceOf(CliTool.Command.Exit.class));
        assertThat(((CliTool.Command.Exit) command).status(), is(CliTool.ExitStatus.USAGE));
    }

    @Test
    public void testUseradd_Parse_NoPassword() throws Exception {
        ESUsersTool tool = new ESUsersTool(new TerminalMock() {
            @Override
            public char[] readSecret(String text, Object... args) {
                return "changeme".toCharArray();
            }
        });
        CliTool.Command command = tool.parse("useradd", args("username"));
        assertThat(command, instanceOf(ESUsersTool.Useradd.class));
        ESUsersTool.Useradd cmd = (ESUsersTool.Useradd) command;
        assertThat(cmd.username, equalTo("username"));
        assertThat(new String(cmd.passwd), equalTo("changeme"));
        assertThat(cmd.roles, notNullValue());
        assertThat(cmd.roles.length, is(0));
    }

    @Test
    public void testUseradd_Cmd_Create() throws Exception {
        Path dir = Files.createTempDirectory(null);
        Path users = dir.resolve("users");
        Path usersRoles = dir.resolve("users_roles");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", users.toAbsolutePath())
                .put("shield.authc.esusers.files.users_roles", usersRoles.toAbsolutePath())
                .build();
        Environment env = new Environment(settings);

        ESUsersTool.Useradd cmd = new ESUsersTool.Useradd(new TerminalMock(), "user1", "changeme".toCharArray(), "r1", "r2");

        CliTool.ExitStatus status = cmd.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertThat(Files.exists(users), is(true));
        List<String> lines = Files.readAllLines(users, Charsets.UTF_8);
        assertThat(lines.size(), is(1));
        // we can't just hash again and compare the lines, as every time we hash a new salt is generated
        // instead we'll just verify the generated hash against the correct password.
        String line = lines.get(0);
        assertThat(line, startsWith("user1:"));
        String hash = line.substring("user1:".length());
        assertThat(Hasher.HTPASSWD.verify("changeme".toCharArray(), hash.toCharArray()), is(true));

        assertThat(Files.exists(usersRoles), is(true));
        lines = Files.readAllLines(usersRoles, Charsets.UTF_8);
        assertThat(lines.size(), is(1));
        line = lines.get(0);
        assertThat(line, equalTo("user1:r1,r2"));
    }

    @Test
    public void testUseradd_Cmd_Append() throws Exception {
        Path users = Files.createTempFile(null, null);
        Path usersRoles = Files.createTempFile(null, null);
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", users.toAbsolutePath())
                .put("shield.authc.esusers.files.users_roles", usersRoles.toAbsolutePath())
                .build();
        Environment env = new Environment(settings);

        try (BufferedWriter writer = Files.newBufferedWriter(users, Charsets.UTF_8)) {
            writer.write("user2:hash2");
            writer.flush();
        }

        try (BufferedWriter writer = Files.newBufferedWriter(usersRoles, Charsets.UTF_8)) {
            writer.write("user2:r3,r4");
            writer.flush();
        }

        ESUsersTool.Useradd cmd = new ESUsersTool.Useradd(new TerminalMock(), "user1", "changeme".toCharArray(), "r1", "r2");

        CliTool.ExitStatus status = cmd.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertThat(Files.exists(users), is(true));
        List<String> lines = Files.readAllLines(users, Charsets.UTF_8);
        assertThat(lines.size(), is(2));
        assertThat(lines.get(0), equalTo("user2:hash2"));
        // we can't just hash again and compare the lines, as every time we hash a new salt is generated
        // instead we'll just verify the generated hash against the correct password.
        String line = lines.get(1);
        assertThat(line, startsWith("user1:"));
        String hash = line.substring("user1:".length());
        assertThat(Hasher.HTPASSWD.verify("changeme".toCharArray(), hash.toCharArray()), is(true));

        assertThat(Files.exists(usersRoles), is(true));
        lines = Files.readAllLines(usersRoles, Charsets.UTF_8);
        assertThat(lines.size(), is(2));
        assertThat(lines.get(0), equalTo("user2:r3,r4"));
        line = lines.get(1);
        assertThat(line, equalTo("user1:r1,r2"));
    }

    @Test
    public void testUseradd_Cmd_Append_UserAlreadyExists() throws Exception {
        Path users = Files.createTempFile(null, null);
        Path usersRoles = Files.createTempFile(null, null);
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", users.toAbsolutePath())
                .put("shield.authc.esusers.files.users_roles", usersRoles.toAbsolutePath())
                .build();
        Environment env = new Environment(settings);

        try (BufferedWriter writer = Files.newBufferedWriter(users, Charsets.UTF_8)) {
            writer.write("user1:hash1");
            writer.flush();
        }

        ESUsersTool.Useradd cmd = new ESUsersTool.Useradd(new TerminalMock(), "user1", "changeme".toCharArray(), "r1", "r2");

        CliTool.ExitStatus status = cmd.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.CODE_ERROR));
    }

    @Test
    public void testUserdel_Parse() throws Exception {
        ESUsersTool tool = new ESUsersTool();
        CliTool.Command command = tool.parse("userdel", args("username"));
        assertThat(command, instanceOf(ESUsersTool.Userdel.class));
        ESUsersTool.Userdel userdel = (ESUsersTool.Userdel) command;
        assertThat(userdel.username, equalTo("username"));
    }

    @Test
    public void testUserdel_Parse_MissingUsername() throws Exception {
        ESUsersTool tool = new ESUsersTool();
        CliTool.Command command = tool.parse("userdel", args(null));
        assertThat(command, instanceOf(ESUsersTool.Command.Exit.class));
        ESUsersTool.Command.Exit exit = (ESUsersTool.Command.Exit) command;
        assertThat(exit.status(), equalTo(CliTool.ExitStatus.USAGE));
    }

    @Test
    public void testUserdel_Cmd() throws Exception {
        Path users = Files.createTempFile(null, null);
        Path usersRoles = Files.createTempFile(null, null);
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", users.toAbsolutePath())
                .put("shield.authc.esusers.files.users_roles", usersRoles.toAbsolutePath())
                .build();
        Environment env = new Environment(settings);

        try (BufferedWriter writer = Files.newBufferedWriter(users, Charsets.UTF_8)) {
            writer.write("user1:hash2");
            writer.flush();
        }

        try (BufferedWriter writer = Files.newBufferedWriter(usersRoles, Charsets.UTF_8)) {
            writer.write("user1:r3,r4");
            writer.flush();
        }

        ESUsersTool.Userdel cmd = new ESUsersTool.Userdel(new TerminalMock(), "user1");

        CliTool.ExitStatus status = cmd.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertThat(Files.exists(users), is(true));
        List<String> lines = Files.readAllLines(users, Charsets.UTF_8);
        assertThat(lines.size(), is(0));

        assertThat(Files.exists(usersRoles), is(true));
        lines = Files.readAllLines(usersRoles, Charsets.UTF_8);
        assertThat(lines.size(), is(0));
    }

    @Test
    public void testUserdel_Cmd_MissingUser() throws Exception {
        Path users = Files.createTempFile(null, null);
        Path usersRoles = Files.createTempFile(null, null);
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", users.toAbsolutePath())
                .put("shield.authc.esusers.files.users_roles", usersRoles.toAbsolutePath())
                .build();
        Environment env = new Environment(settings);

        try (BufferedWriter writer = Files.newBufferedWriter(users, Charsets.UTF_8)) {
            writer.write("user1:hash2");
            writer.flush();
        }

        try (BufferedWriter writer = Files.newBufferedWriter(usersRoles, Charsets.UTF_8)) {
            writer.write("user1:r3,r4");
            writer.flush();
        }

        ESUsersTool.Userdel cmd = new ESUsersTool.Userdel(new TerminalMock(), "user2");

        CliTool.ExitStatus status = cmd.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertThat(Files.exists(users), is(true));
        List<String> lines = Files.readAllLines(users, Charsets.UTF_8);
        assertThat(lines.size(), is(1));

        assertThat(Files.exists(usersRoles), is(true));
        lines = Files.readAllLines(usersRoles, Charsets.UTF_8);
        assertThat(lines.size(), is(1));
    }

    @Test
    public void testUserdel_Cmd_MissingFiles() throws Exception {
        Path dir = Files.createTempDirectory(null);
        Path users = dir.resolve("users");
        Path usersRoles = dir.resolve("users_roles");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", users.toAbsolutePath())
                .put("shield.authc.esusers.files.users_roles", usersRoles.toAbsolutePath())
                .build();
        Environment env = new Environment(settings);

        ESUsersTool.Userdel cmd = new ESUsersTool.Userdel(new TerminalMock(), "user2");

        CliTool.ExitStatus status = cmd.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertThat(Files.exists(users), is(false));
        assertThat(Files.exists(usersRoles), is(false));
    }

    @Test
    public void testPasswd_Parse_AllOptions() throws Exception {
        ESUsersTool tool = new ESUsersTool();
        CliTool.Command command = tool.parse("passwd", args("user1 -p changeme"));
        assertThat(command, instanceOf(ESUsersTool.Passwd.class));
        ESUsersTool.Passwd cmd = (ESUsersTool.Passwd) command;
        assertThat(cmd.username, equalTo("user1"));
        assertThat(new String(cmd.passwd), equalTo("changeme"));
    }

    @Test
    public void testPasswd_Parse_MissingUsername() throws Exception {
        ESUsersTool tool = new ESUsersTool();
        CliTool.Command command = tool.parse("passwd", args("-p changeme"));
        assertThat(command, instanceOf(ESUsersTool.Command.Exit.class));
        ESUsersTool.Command.Exit cmd = (ESUsersTool.Command.Exit) command;
        assertThat(cmd.status(), is(CliTool.ExitStatus.USAGE));
    }

    @Test
    public void testPasswd_Parse_MissingPassword() throws Exception {
        final AtomicReference<Boolean> secretRequested = new AtomicReference<>(false);
        Terminal terminal = new TerminalMock() {
            @Override
            public char[] readSecret(String text, Object... args) {
                secretRequested.set(true);
                return "changeme".toCharArray();
            }
        };
        ESUsersTool tool = new ESUsersTool(terminal);
        CliTool.Command command = tool.parse("passwd", args("user1"));
        assertThat(command, instanceOf(ESUsersTool.Passwd.class));
        ESUsersTool.Passwd cmd = (ESUsersTool.Passwd) command;
        assertThat(cmd.username, equalTo("user1"));
        assertThat(new String(cmd.passwd), equalTo("changeme"));
        assertThat(secretRequested.get(), is(true));
    }

    @Test
    public void testPasswd_Cmd() throws Exception {
        Path users = Files.createTempFile(null, null);
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", users.toAbsolutePath())
                .build();
        Environment env = new Environment(settings);

        try (BufferedWriter writer = Files.newBufferedWriter(users, Charsets.UTF_8)) {
            writer.write("user1:hash2");
            writer.flush();
        }

        ESUsersTool.Passwd cmd = new ESUsersTool.Passwd(new TerminalMock(), "user1", "changeme".toCharArray());
        CliTool.ExitStatus status = cmd.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.OK));

        List<String> lines = Files.readAllLines(users, Charsets.UTF_8);
        assertThat(lines.size(), is(1));
        // we can't just hash again and compare the lines, as every time we hash a new salt is generated
        // instead we'll just verify the generated hash against the correct password.
        String line = lines.get(0);
        assertThat(line, startsWith("user1:"));
        String hash = line.substring("user1:".length());
        assertThat(Hasher.HTPASSWD.verify("changeme".toCharArray(), hash.toCharArray()), is(true));
    }

    @Test
    public void testPasswd_Cmd_UnknownUser() throws Exception {
        Path users = Files.createTempFile(null, null);
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", users.toAbsolutePath())
                .build();
        Environment env = new Environment(settings);

        try (BufferedWriter writer = Files.newBufferedWriter(users, Charsets.UTF_8)) {
            writer.write("user1:hash2");
            writer.flush();
        }

        ESUsersTool.Passwd cmd = new ESUsersTool.Passwd(new TerminalMock(), "user2", "changeme".toCharArray());
        CliTool.ExitStatus status = cmd.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.NO_USER));
    }

    @Test
    public void testPasswd_Cmd_MissingFiles() throws Exception {
        Path dir = Files.createTempDirectory(null);
        Path users = dir.resolve("users");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", users.toAbsolutePath())
                .build();
        Environment env = new Environment(settings);

        ESUsersTool.Passwd cmd = new ESUsersTool.Passwd(new TerminalMock(), "user2", "changeme".toCharArray());
        CliTool.ExitStatus status = cmd.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.NO_USER));
    }

}
