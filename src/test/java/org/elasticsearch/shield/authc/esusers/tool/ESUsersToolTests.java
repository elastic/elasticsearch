/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers.tool;

import com.google.common.io.Files;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.esusers.FileUserRolesStore;
import org.elasticsearch.shield.authc.support.Hasher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class ESUsersToolTests extends CliToolTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
        ESUsersTool tool = new ESUsersTool(new MockTerminal() {
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
        File tmpFolder = temporaryFolder.newFolder();
        File userFile = new File(tmpFolder, "users");
        File userRolesFile = new File(tmpFolder, "users_roles");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", userFile)
                .put("shield.authc.esusers.files.users_roles", userRolesFile)
                .build();

        ESUsersTool.Useradd cmd = new ESUsersTool.Useradd(new MockTerminal(), "user1", "changeme".toCharArray(), "r1", "r2");

        CliTool.ExitStatus status = execute(cmd, settings);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertFileExists(userFile);
        List<String> lines = Files.readLines(userFile, Charsets.UTF_8);
        assertThat(lines.size(), is(1));
        // we can't just hash again and compare the lines, as every time we hash a new salt is generated
        // instead we'll just verify the generated hash against the correct password.
        String line = lines.get(0);
        assertThat(line, startsWith("user1:"));
        String hash = line.substring("user1:".length());
        assertThat(Hasher.HTPASSWD.verify("changeme".toCharArray(), hash.toCharArray()), is(true));

        assertFileExists(userRolesFile);
        lines = Files.readLines(userRolesFile, Charsets.UTF_8);
        assertThat(lines.size(), is(1));
        line = lines.get(0);
        assertThat(line, equalTo("user1:r1,r2"));
    }

    @Test
    public void testUseradd_Cmd_Append() throws Exception {
        File userFile = writeFile("user2:hash2");
        File userRolesFile = writeFile("user2:r3,r4");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", userFile)
                .put("shield.authc.esusers.files.users_roles", userRolesFile)
                .build();

        ESUsersTool.Useradd cmd = new ESUsersTool.Useradd(new MockTerminal(), "user1", "changeme".toCharArray(), "r1", "r2");

        CliTool.ExitStatus status = execute(cmd, settings);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertFileExists(userFile);
        List<String> lines = Files.readLines(userFile, Charsets.UTF_8);
        assertThat(lines.size(), is(2));
        assertThat(lines.get(0), equalTo("user2:hash2"));
        // we can't just hash again and compare the lines, as every time we hash a new salt is generated
        // instead we'll just verify the generated hash against the correct password.
        String line = lines.get(1);
        assertThat(line, startsWith("user1:"));
        String hash = line.substring("user1:".length());
        assertThat(Hasher.HTPASSWD.verify("changeme".toCharArray(), hash.toCharArray()), is(true));

        assertFileExists(userRolesFile);
        lines = Files.readLines(userRolesFile, Charsets.UTF_8);
        assertThat(lines.size(), is(2));
        assertThat(lines.get(0), equalTo("user2:r3,r4"));
        line = lines.get(1);
        assertThat(line, equalTo("user1:r1,r2"));
    }

    @Test
    public void testUseradd_Cmd_Append_UserAlreadyExists() throws Exception {
        File userFile = writeFile("user1:hash1");
        File userRolesFile = temporaryFolder.newFile();
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", userFile)
                .put("shield.authc.esusers.files.users_roles", userRolesFile)
                .build();

        ESUsersTool.Useradd cmd = new ESUsersTool.Useradd(new MockTerminal(), "user1", "changeme".toCharArray(), "r1", "r2");

        CliTool.ExitStatus status = execute(cmd, settings);
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
        File userFile = writeFile("user1:hash2");
        File userRolesFile = writeFile("user1:r3,r4");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", userFile)
                .put("shield.authc.esusers.files.users_roles", userRolesFile)
                .build();

        ESUsersTool.Userdel cmd = new ESUsersTool.Userdel(new MockTerminal(), "user1");

        CliTool.ExitStatus status = execute(cmd, settings);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertFileExists(userFile); 
        List<String> lines = Files.readLines(userFile, Charsets.UTF_8);
        assertThat(lines.size(), is(0));

        assertFileExists(userRolesFile);
        lines = Files.readLines(userRolesFile, Charsets.UTF_8);
        assertThat(lines.size(), is(0));
    }

    @Test
    public void testUserdel_Cmd_MissingUser() throws Exception {
        File userFile = writeFile("user1:hash2");
        File userRolesFile = writeFile("user1:r3,r4");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", userFile)
                .put("shield.authc.esusers.files.users_roles", userRolesFile)
                .build();

        ESUsersTool.Userdel cmd = new ESUsersTool.Userdel(new MockTerminal(), "user2");

        CliTool.ExitStatus status = execute(cmd, settings);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertFileExists(userFile); 
        List<String> lines = Files.readLines(userFile, Charsets.UTF_8);
        assertThat(lines.size(), is(1));

        assertFileExists(userRolesFile);
        lines = Files.readLines(userRolesFile, Charsets.UTF_8);
        assertThat(lines.size(), is(1));
    }

    @Test
    public void testUserdel_Cmd_MissingFiles() throws Exception {
        File dir = temporaryFolder.newFolder();
        File userFile = new File(dir, "users");
        File userRolesFile = new File(dir, "users_roles");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", userFile)
                .put("shield.authc.esusers.files.users_roles", userRolesFile)
                .build();

        ESUsersTool.Userdel cmd = new ESUsersTool.Userdel(new MockTerminal(), "user2");

        CliTool.ExitStatus status = execute(cmd, settings);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertThat(userFile.exists(), is(false));
        assertThat(userRolesFile.exists(), is(false));
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
        Terminal terminal = new MockTerminal() {
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
        File userFile = writeFile("user1:hash2");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", userFile)
                .build();

        ESUsersTool.Passwd cmd = new ESUsersTool.Passwd(new MockTerminal(), "user1", "changeme".toCharArray());
        CliTool.ExitStatus status = execute(cmd, settings);
        assertThat(status, is(CliTool.ExitStatus.OK));

        List<String> lines = Files.readLines(userFile, Charsets.UTF_8);
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
        File userFile = writeFile("user1:hash2");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", userFile)
                .build();

        ESUsersTool.Passwd cmd = new ESUsersTool.Passwd(new MockTerminal(), "user2", "changeme".toCharArray());
        CliTool.ExitStatus status = execute(cmd, settings);
        assertThat(status, is(CliTool.ExitStatus.NO_USER));
    }

    @Test
    public void testPasswd_Cmd_MissingFiles() throws Exception {
        File userFile = temporaryFolder.newFile();
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", userFile)
                .build();

        ESUsersTool.Passwd cmd = new ESUsersTool.Passwd(new MockTerminal(), "user2", "changeme".toCharArray());
        CliTool.ExitStatus status = execute(cmd, settings);
        assertThat(status, is(CliTool.ExitStatus.NO_USER));
    }

    @Test
    public void testRoles_Parse_AllOptions() throws Exception {
        ESUsersTool tool = new ESUsersTool();
        CliTool.Command command = tool.parse("roles", args("someuser -a test1,test2,test3 -r test4,test5,test6"));
        assertThat(command, instanceOf(ESUsersTool.Roles.class));
        ESUsersTool.Roles rolesCommand = (ESUsersTool.Roles) command;
        assertThat(rolesCommand.username, is("someuser"));
        assertThat(rolesCommand.addRoles, arrayContaining("test1", "test2", "test3"));
        assertThat(rolesCommand.removeRoles, arrayContaining("test4", "test5", "test6"));
    }

    @Test
    public void testRoles_Cmd_validatingRoleNames() throws Exception {
        ESUsersTool tool = new ESUsersTool();
        File usersFile = writeFile("admin:hash");
        File usersRoleFile = writeFile("admin: admin\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", usersFile)
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .build();

        // invalid role names
        assertThat(execute(tool.parse("roles", args("admin -a r0le!")), settings), is(CliTool.ExitStatus.DATA_ERROR));
        assertThat(execute(tool.parse("roles", args("admin -a role%")), settings), is(CliTool.ExitStatus.DATA_ERROR));
        assertThat(execute(tool.parse("roles", args("admin -a role:")), settings), is(CliTool.ExitStatus.DATA_ERROR));
        assertThat(execute(tool.parse("roles", args("admin -a role>")), settings), is(CliTool.ExitStatus.DATA_ERROR));

        // valid role names
        assertThat(execute(tool.parse("roles", args("admin -a test01")), settings), is(CliTool.ExitStatus.OK));
        assertThat(execute(tool.parse("roles", args("admin -a @role")), settings), is(CliTool.ExitStatus.OK));
        assertThat(execute(tool.parse("roles", args("admin -a _role")), settings), is(CliTool.ExitStatus.OK));
        assertThat(execute(tool.parse("roles", args("admin -a -role")), settings), is(CliTool.ExitStatus.OK));
        assertThat(execute(tool.parse("roles", args("admin -a -Role")), settings), is(CliTool.ExitStatus.OK));
        assertThat(execute(tool.parse("roles", args("admin -a role0")), settings), is(CliTool.ExitStatus.OK));
    }

    @Test
    public void testRoles_Cmd_addingRoleWorks() throws Exception {
        File usersRoleFile = writeFile("admin: admin\nuser:user\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .build();

        ESUsersTool.Roles cmd = new ESUsersTool.Roles(new MockTerminal(), "user", new String[]{"foo"}, Strings.EMPTY_ARRAY);
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.OK));

        Map<String, String[]> userRoles = FileUserRolesStore.parseFile(usersRoleFile.toPath(), logger);
        assertThat(userRoles.keySet(), hasSize(2));
        assertThat(userRoles.keySet(), hasItems("admin", "user"));
        assertThat(userRoles.get("admin"), arrayContaining("admin"));
        assertThat(userRoles.get("user"), arrayContaining("user", "foo"));
    }

    @Test
    public void testRoles_Cmd_removingRoleWorks() throws Exception {
        File usersRoleFile = writeFile("admin: admin\nuser:user,foo,bar\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .build();

        ESUsersTool.Roles cmd = new ESUsersTool.Roles(new MockTerminal(), "user", Strings.EMPTY_ARRAY, new String[]{"foo"});
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.OK));

        Map<String, String[]> userRoles = FileUserRolesStore.parseFile(usersRoleFile.toPath(), logger);
        assertThat(userRoles.keySet(), hasSize(2));
        assertThat(userRoles.keySet(), hasItems("admin", "user"));
        assertThat(userRoles.get("admin"), arrayContaining("admin"));
        assertThat(userRoles.get("user"), arrayContaining("user", "bar"));
    }

    @Test
    public void testRoles_Cmd_addingAndRemovingRoleWorks() throws Exception {
        File usersRoleFile = writeFile("admin: admin\nuser:user,foo,bar\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .build();

        ESUsersTool.Roles cmd = new ESUsersTool.Roles(new MockTerminal(), "user", new String[]{"newrole"}, new String[]{"foo"});
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.OK));

        Map<String, String[]> userRoles = FileUserRolesStore.parseFile(usersRoleFile.toPath(), logger);
        assertThat(userRoles.keySet(), hasSize(2));
        assertThat(userRoles.keySet(), hasItems("admin", "user"));
        assertThat(userRoles.get("admin"), arrayContaining("admin"));
        assertThat(userRoles.get("user"), arrayContaining("user", "bar", "newrole"));
    }

    @Test
    public void testRoles_Cmd_userNotFound() throws Exception {
        File usersRoleFile = writeFile("admin: admin\nuser:user,foo,bar\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .build();

        ESUsersTool.Roles cmd = new ESUsersTool.Roles(new MockTerminal(), "does-not-exist", Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.NO_USER));
    }

    @Test
    public void testRoles_Cmd_testNotAddingOrRemovingRolesShowsListingOfRoles() throws Exception {
        File usersFile = writeFile("admin:hash\nuser:hash");
        File usersRoleFile = writeFile("admin: admin\nuser:user,foo,bar\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users", usersFile)
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .build();

        CaptureOutputTerminal catchTerminalOutput = new CaptureOutputTerminal();
        ESUsersTool.Roles cmd = new ESUsersTool.Roles(catchTerminalOutput, "user", Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.OK));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasItem(allOf(containsString("user"), containsString("user,foo,bar"))));
    }

    @Test
    public void testListUsersAndRoles_Cmd_parsingWorks() throws Exception {
        ESUsersTool tool = new ESUsersTool();
        CliTool.Command command = tool.parse("list", args("someuser"));
        assertThat(command, instanceOf(ESUsersTool.ListUsersAndRoles.class));
        ESUsersTool.ListUsersAndRoles listUsersAndRolesCommand = (ESUsersTool.ListUsersAndRoles) command;
        assertThat(listUsersAndRolesCommand.username, is("someuser"));
    }

    @Test
    public void testListUsersAndRoles_Cmd_listAllUsers() throws Exception {
        File usersRoleFile = writeFile("admin: admin\nuser: user,foo,bar\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .build();

        CaptureOutputTerminal catchTerminalOutput = new CaptureOutputTerminal();
        ESUsersTool.ListUsersAndRoles cmd = new ESUsersTool.ListUsersAndRoles(catchTerminalOutput, null);
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.OK));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasSize(greaterThanOrEqualTo(2)));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasItem(containsString("admin")));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasItem(allOf(containsString("user"), containsString("user,foo,bar"))));
    }

    @Test
    public void testListUsersAndRoles_Cmd_listSingleUser() throws Exception {
        File usersRoleFile = writeFile("admin: admin\nuser: user,foo,bar\n");
        File usersFile = writeFile("admin:{plain}changeme\nuser:{plain}changeme\nno-roles-user:{plain}changeme\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .put("shield.authc.esusers.files.users", usersFile)
                .build();

        CaptureOutputTerminal catchTerminalOutput = new CaptureOutputTerminal();
        ESUsersTool.ListUsersAndRoles cmd = new ESUsersTool.ListUsersAndRoles(catchTerminalOutput, "admin");
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.OK));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasSize(greaterThanOrEqualTo(1)));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasItem(containsString("admin")));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasItem(not(containsString("user"))));
    }

    @Test
    public void testListUsersAndRoles_Cmd_listSingleUserNotFound() throws Exception {
        File usersRoleFile = writeFile("admin: admin\nuser: user,foo,bar\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .build();

        CaptureOutputTerminal catchTerminalOutput = new CaptureOutputTerminal();
        ESUsersTool.ListUsersAndRoles cmd = new ESUsersTool.ListUsersAndRoles(catchTerminalOutput, "does-not-exist");
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.NO_USER));
    }

    @Test
    public void testListUsersAndRoles_Cmd_testThatUsersWithoutRolesAreListed() throws Exception {
        File usersFile = writeFile("admin:{plain}changeme\nuser:{plain}changeme\nno-roles-user:{plain}changeme\n");
        File usersRoleFile = writeFile("admin: admin\nuser: user,foo,bar\n");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .put("shield.authc.esusers.files.users", usersFile)
                .build();

        CaptureOutputTerminal catchTerminalOutput = new CaptureOutputTerminal();
        ESUsersTool.ListUsersAndRoles cmd = new ESUsersTool.ListUsersAndRoles(catchTerminalOutput, null);
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.OK));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasSize(greaterThanOrEqualTo(3)));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasItem(containsString("admin")));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasItem(allOf(containsString("user"), containsString("user,foo,bar"))));
        assertThat(catchTerminalOutput.getTerminalOutput(), hasItem(allOf(containsString("no-roles-user"), containsString("-"))));
    }

    @Test
    public void testListUsersAndRoles_Cmd_testThatUsersWithoutRolesAreListedForSingleUser() throws Exception {
        File usersFile = writeFile("admin:{plain}changeme");
        File usersRoleFile = writeFile("");
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.files.users_roles", usersRoleFile)
                .put("shield.authc.esusers.files.users", usersFile)
                .build();

        CaptureOutputTerminal loggingTerminal = new CaptureOutputTerminal();
        ESUsersTool.ListUsersAndRoles cmd = new ESUsersTool.ListUsersAndRoles(loggingTerminal, "admin");
        CliTool.ExitStatus status = execute(cmd, settings);

        assertThat(status, is(CliTool.ExitStatus.OK));
        assertThat(loggingTerminal.getTerminalOutput(), hasSize(greaterThanOrEqualTo(1)));
        assertThat(loggingTerminal.getTerminalOutput(), hasItem(allOf(containsString("admin"), containsString("-"))));
    }

    private CliTool.ExitStatus execute(CliTool.Command cmd, Settings settings) throws Exception {
        Environment env = new Environment(settings);
        return cmd.execute(settings, env);
    }

    private File writeFile(String content) throws IOException {
        File file = temporaryFolder.newFile();
        Files.write(content.getBytes(Charsets.UTF_8), file);
        return file;
    }

    private void assertFileExists(File file) {
        assertThat(String.format(Locale.ROOT, "Expected file [%s] to exist", file), file.exists(), is(true));
    }
}
