/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.file.tool;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.hamcrest.Matchers.containsString;

public class UsersToolTests extends CommandTestCase {

    // the mock filesystem we use so permissions/users/groups can be modified
    static FileSystem jimfs;
    String pathHomeParameter;
    String fileOrderParameter;

    // the config dir for each test to use
    Path confDir;

    // settings used to create an Environment for tools
    Settings settings;

    Hasher hasher;

    @BeforeClass
    public static void setupJimfs() throws IOException {
        String view = randomFrom("basic", "posix");
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews(view).build();
        jimfs = Jimfs.newFileSystem(conf);
        PathUtilsForTesting.installMock(jimfs);
    }

    @Before
    public void setupHome() throws IOException {
        Path homeDir = jimfs.getPath("eshome");
        IOUtils.rm(homeDir);
        confDir = homeDir.resolve("config");
        Files.createDirectories(confDir);
        hasher = getFastStoredHashAlgoForTests();
        String defaultPassword = SecuritySettingsSourceField.TEST_PASSWORD;
        Files.write(
            confDir.resolve("users"),
            Arrays.asList(
                "existing_user:" + new String(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)),
                "existing_user2:" + new String(hasher.hash(new SecureString((defaultPassword + "2").toCharArray()))),
                "existing_user3:" + new String(hasher.hash(new SecureString((defaultPassword + "3").toCharArray())))
            ),
            StandardCharsets.UTF_8
        );
        Files.write(
            confDir.resolve("users_roles"),
            Arrays.asList("test_admin:existing_user,existing_user2", "test_r1:existing_user2"),
            StandardCharsets.UTF_8
        );
        Files.write(
            confDir.resolve("roles.yml"),
            Arrays.asList("test_admin:", "  cluster: all", "test_r1:", "  cluster: all", "test_r2:", "  cluster: all"),
            StandardCharsets.UTF_8
        );
        settings = Settings.builder()
            .put("path.home", homeDir)
            .put("xpack.security.authc.realms.file.file.order", 0)
            .put("xpack.security.authc.password_hashing.algorithm", hasher.name())
            .build();
        pathHomeParameter = "-Epath.home=" + homeDir;
        fileOrderParameter = "-Expack.security.authc.realms.file.file.order=0";
    }

    @AfterClass
    public static void closeJimfs() throws IOException {
        if (jimfs != null) {
            jimfs.close();
            jimfs = null;
        }
    }

    @Override
    protected Command newCommand() {
        return new UsersTool() {
            @Override
            protected AddUserCommand newAddUserCommand() {
                return new AddUserCommand() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        return new Environment(UsersToolTests.this.settings, confDir);
                    }
                };
            }

            @Override
            protected DeleteUserCommand newDeleteUserCommand() {
                return new DeleteUserCommand() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        return new Environment(UsersToolTests.this.settings, confDir);
                    }
                };
            }

            @Override
            protected PasswordCommand newPasswordCommand() {
                return new PasswordCommand() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        return new Environment(UsersToolTests.this.settings, confDir);
                    }
                };
            }

            @Override
            protected RolesCommand newRolesCommand() {
                return new RolesCommand() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        return new Environment(UsersToolTests.this.settings, confDir);
                    }
                };
            }

            @Override
            protected ListCommand newListCommand() {
                return new ListCommand() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        return new Environment(UsersToolTests.this.settings, confDir);
                    }
                };
            }
        };
    }

    /** checks the user exists with the given password */
    void assertUser(String username, String password) throws IOException {
        List<String> lines = Files.readAllLines(confDir.resolve("users"), StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] usernameHash = line.split(":", 2);
            if (usernameHash.length != 2) {
                fail("Corrupted users file, line: " + line);
            }
            if (username.equals(usernameHash[0]) == false) {
                continue;
            }
            String gotHash = usernameHash[1];
            SecureString expectedHash = new SecureString(password.toCharArray());
            assertTrue("Could not validate password for user", hasher.verify(expectedHash, gotHash.toCharArray()));
            return;
        }
        fail("Could not find username " + username + " in users file:\n" + lines.toString());
    }

    /** Checks the user does not exist in the users or users_roles files*/
    void assertNoUser(String username) throws IOException {
        List<String> lines = Files.readAllLines(confDir.resolve("users"), StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] usernameHash = line.split(":", 2);
            if (usernameHash.length != 2) {
                fail("Corrupted users file, line: " + line);
            }
            assertNotEquals(username, usernameHash[0]);
        }
        lines = Files.readAllLines(confDir.resolve("users_roles"), StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] roleUsers = line.split(":", 2);
            if (roleUsers.length != 2) {
                fail("Corrupted users_roles file, line: " + line);
            }
            String[] users = roleUsers[1].split(",");
            for (String user : users) {
                assertNotEquals(user, username);
            }
        }

    }

    /** checks the role has the given users, or that the role does not exist if not users are passed. */
    void assertRole(String role, String... users) throws IOException {
        List<String> lines = Files.readAllLines(confDir.resolve("users_roles"), StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] roleUsers = line.split(":", 2);
            if (roleUsers.length != 2) {
                fail("Corrupted users_roles file, line: " + line);
            }
            if (role.equals(roleUsers[0]) == false) {
                continue;
            }
            if (users.length == 0) {
                fail("Found role " + role + " in users_roles file with users [" + roleUsers[1] + "]");
            }
            List<String> gotUsers = Arrays.asList(roleUsers[1].split(","));
            for (String user : users) {
                if (gotUsers.contains(user) == false) {
                    fail("Expected users [" + Arrays.toString(users) + "] for role " + role + " but found [" + gotUsers.toString() + "]");
                }
            }
            return;
        }
        if (users.length != 0) {
            fail("Could not find role " + role + " in users_roles file:\n" + lines.toString());
        }
    }

    public void testParseInvalidUsername() throws Exception {
        UserException e = expectThrows(
            UserException.class,
            () -> { UsersTool.parseUsername(Collections.singletonList("áccented"), Settings.EMPTY); }
        );
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("Invalid username"));
    }

    public void testParseReservedUsername() throws Exception {
        final String name = randomFrom(ElasticUser.NAME, KibanaUser.NAME);
        UserException e = expectThrows(
            UserException.class,
            () -> { UsersTool.parseUsername(Collections.singletonList(name), Settings.EMPTY); }
        );
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("Invalid username"));

        Settings settings = Settings.builder().put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false).build();
        UsersTool.parseUsername(Collections.singletonList(name), settings);
    }

    public void testParseUsernameMissing() throws Exception {
        UserException e = expectThrows(UserException.class, () -> { UsersTool.parseUsername(Collections.emptyList(), Settings.EMPTY); });
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("Missing username argument"));
    }

    public void testParseUsernameExtraArgs() throws Exception {
        UserException e = expectThrows(
            UserException.class,
            () -> { UsersTool.parseUsername(Arrays.asList("username", "extra"), Settings.EMPTY); }
        );
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("Expected a single username argument"));
    }

    public void testParseInvalidPasswordOption() throws Exception {
        UserException e = expectThrows(UserException.class, () -> { UsersTool.parsePassword(terminal, "123"); });
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("Invalid password"));
    }

    public void testParseInvalidPasswordInput() throws Exception {
        terminal.addSecretInput("123");
        UserException e = expectThrows(UserException.class, () -> { UsersTool.parsePassword(terminal, null); });
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("Invalid password"));
    }

    public void testParseMismatchPasswordInput() throws Exception {
        terminal.addSecretInput("password1");
        terminal.addSecretInput("password2");
        UserException e = expectThrows(UserException.class, () -> { UsersTool.parsePassword(terminal, null); });
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("Password mismatch"));
    }

    public void testParseUnknownRole() throws Exception {
        UsersTool.parseRoles(terminal, TestEnvironment.newEnvironment(settings), "test_r1,r2,r3");
        String output = terminal.getErrorOutput();
        assertTrue(output, output.contains("The following roles [r2,r3] are not in the ["));
    }

    public void testParseReservedRole() throws Exception {
        final String reservedRoleName = randomFrom(ReservedRolesStore.names().toArray(Strings.EMPTY_ARRAY));
        String rolesArg = randomBoolean() ? "test_r1," + reservedRoleName : reservedRoleName;
        UsersTool.parseRoles(terminal, TestEnvironment.newEnvironment(settings), rolesArg);
        String output = terminal.getOutput();
        assertTrue(output, output.isEmpty());
    }

    public void testParseInvalidRole() throws Exception {
        UserException e = expectThrows(
            UserException.class,
            () -> { UsersTool.parseRoles(terminal, TestEnvironment.newEnvironment(settings), "fóóbár"); }
        );
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("Invalid role [fóóbár]"));
    }

    public void testParseMultipleRoles() throws Exception {
        String[] roles = UsersTool.parseRoles(terminal, TestEnvironment.newEnvironment(settings), "test_r1,test_r2");
        assertEquals(Objects.toString(roles), 2, roles.length);
        assertEquals("test_r1", roles[0]);
        assertEquals("test_r2", roles[1]);
    }

    public void testUseraddNoPassword() throws Exception {
        terminal.addSecretInput(SecuritySettingsSourceField.TEST_PASSWORD);
        terminal.addSecretInput(SecuritySettingsSourceField.TEST_PASSWORD);
        execute("useradd", pathHomeParameter, fileOrderParameter, "username");
        assertUser("username", SecuritySettingsSourceField.TEST_PASSWORD);
    }

    public void testUseraddPasswordOption() throws Exception {
        execute("useradd", pathHomeParameter, fileOrderParameter, "username", "-p", SecuritySettingsSourceField.TEST_PASSWORD);
        assertUser("username", SecuritySettingsSourceField.TEST_PASSWORD);
    }

    public void testUseraddUserExists() throws Exception {
        UserException e = expectThrows(
            UserException.class,
            () -> {
                execute("useradd", pathHomeParameter, fileOrderParameter, "existing_user", "-p", SecuritySettingsSourceField.TEST_PASSWORD);
            }
        );
        assertEquals(ExitCodes.CODE_ERROR, e.exitCode);
        assertEquals("User [existing_user] already exists", e.getMessage());
    }

    public void testUseraddReservedUser() throws Exception {
        final String name = randomFrom(ElasticUser.NAME, KibanaUser.NAME);
        UserException e = expectThrows(
            UserException.class,
            () -> { execute("useradd", pathHomeParameter, fileOrderParameter, name, "-p", SecuritySettingsSourceField.TEST_PASSWORD); }
        );
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertEquals("Invalid username [" + name + "]... Username [" + name + "] is reserved and may not be used.", e.getMessage());
    }

    public void testUseraddNoRoles() throws Exception {
        Files.delete(confDir.resolve("users_roles"));
        Files.createFile(confDir.resolve("users_roles"));
        execute("useradd", pathHomeParameter, fileOrderParameter, "username", "-p", SecuritySettingsSourceField.TEST_PASSWORD);
        List<String> lines = Files.readAllLines(confDir.resolve("users_roles"), StandardCharsets.UTF_8);
        assertTrue(lines.toString(), lines.isEmpty());
    }

    public void testAddUserWithInvalidHashingAlgorithmInFips() throws Exception {
        settings = Settings.builder()
            .put(settings)
            .put("xpack.security.authc.password_hashing.algorithm", "bcrypt")
            .put("xpack.security.fips_mode.enabled", true)
            .build();

        UserException e = expectThrows(
            UserException.class,
            () -> {
                execute(
                    "useradd",
                    pathHomeParameter,
                    fileOrderParameter,
                    randomAlphaOfLength(12),
                    "-p",
                    SecuritySettingsSourceField.TEST_PASSWORD
                );
            }
        );
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertEquals(
            "Only PBKDF2 is allowed for password hashing in a FIPS 140 JVM. "
                + "Please set the appropriate value for [ xpack.security.authc.password_hashing.algorithm ] setting.",
            e.getMessage()
        );
    }

    public void testUserdelUnknownUser() throws Exception {
        UserException e = expectThrows(
            UserException.class,
            () -> { execute("userdel", pathHomeParameter, fileOrderParameter, "unknown"); }
        );
        assertEquals(ExitCodes.NO_USER, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("User [unknown] doesn't exist"));
    }

    public void testUserdel() throws Exception {
        execute("userdel", pathHomeParameter, fileOrderParameter, "existing_user");
        assertNoUser("existing_user");
    }

    public void testPasswdUnknownUser() throws Exception {
        UserException e = expectThrows(
            UserException.class,
            () -> { execute("passwd", pathHomeParameter, fileOrderParameter, "unknown", "-p", SecuritySettingsSourceField.TEST_PASSWORD); }
        );
        assertEquals(ExitCodes.NO_USER, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("User [unknown] doesn't exist"));
    }

    public void testPasswdNoPasswordOption() throws Exception {
        terminal.addSecretInput("new-test-user-password");
        terminal.addSecretInput("new-test-user-password");
        execute("passwd", pathHomeParameter, fileOrderParameter, "existing_user");
        assertUser("existing_user", "new-test-user-password");
        assertRole("test_admin", "existing_user", "existing_user2"); // roles unchanged
    }

    public void testPasswd() throws Exception {
        execute("passwd", pathHomeParameter, fileOrderParameter, "existing_user", "-p", "new-test-user-password");
        assertUser("existing_user", "new-test-user-password");
        assertRole("test_admin", "existing_user"); // roles unchanged
    }

    public void testPasswdWithInvalidHashingAlgorithmInFips() throws Exception {
        settings = Settings.builder()
            .put(settings)
            .put("xpack.security.authc.password_hashing.algorithm", "bcrypt")
            .put("xpack.security.fips_mode.enabled", true)
            .build();
        UserException e = expectThrows(
            UserException.class,
            () -> { execute("passwd", pathHomeParameter, fileOrderParameter, "existing_user", "-p", "new-test-user-password"); }
        );
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertEquals(
            "Only PBKDF2 is allowed for password hashing in a FIPS 140 JVM. "
                + "Please set the appropriate value for [ xpack.security.authc.password_hashing.algorithm ] setting.",
            e.getMessage()
        );
    }

    public void testRolesUnknownUser() throws Exception {
        UserException e = expectThrows(UserException.class, () -> { execute("roles", pathHomeParameter, fileOrderParameter, "unknown"); });
        assertEquals(ExitCodes.NO_USER, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("User [unknown] doesn't exist"));
    }

    public void testRolesAdd() throws Exception {
        execute("roles", pathHomeParameter, fileOrderParameter, "existing_user", "-a", "test_r1");
        assertRole("test_admin", "existing_user");
        assertRole("test_r1", "existing_user");
    }

    public void testRolesRemove() throws Exception {
        execute("roles", pathHomeParameter, fileOrderParameter, "existing_user", "-r", "test_admin");
        assertRole("test_admin", "existing_user2");
    }

    public void testRolesAddAndRemove() throws Exception {
        execute("roles", pathHomeParameter, fileOrderParameter, "existing_user", "-a", "test_r1", "-r", "test_admin");
        assertRole("test_admin", "existing_user2");
        assertRole("test_r1", "existing_user");
    }

    public void testRolesRemoveLeavesExisting() throws Exception {
        execute(
            "useradd",
            pathHomeParameter,
            fileOrderParameter,
            "username",
            "-p",
            SecuritySettingsSourceField.TEST_PASSWORD,
            "-r",
            "test_admin"
        );
        execute("roles", pathHomeParameter, fileOrderParameter, "existing_user", "-r", "test_admin");
        assertRole("test_admin", "username");
    }

    public void testRolesNoAddOrRemove() throws Exception {
        String output = execute("roles", pathHomeParameter, fileOrderParameter, "existing_user");
        assertTrue(output, output.contains("existing_user"));
        assertTrue(output, output.contains("test_admin"));
    }

    public void testListUnknownUser() throws Exception {
        UserException e = expectThrows(UserException.class, () -> { execute("list", pathHomeParameter, fileOrderParameter, "unknown"); });
        assertEquals(ExitCodes.NO_USER, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("User [unknown] doesn't exist"));
    }

    public void testListAllUsers() throws Exception {
        String output = execute("list", pathHomeParameter, fileOrderParameter);
        assertTrue(output, output.contains("existing_user"));
        assertTrue(output, output.contains("test_admin"));
        assertTrue(output, output.contains("existing_user2"));
        assertTrue(output, output.contains("test_r1"));

        // output should not contain '*' which indicates unknown role
        assertFalse(output, output.contains("*"));
    }

    public void testListSingleUser() throws Exception {
        String output = execute("list", pathHomeParameter, fileOrderParameter, "existing_user");
        assertTrue(output, output.contains("existing_user"));
        assertTrue(output, output.contains("test_admin"));
        assertFalse(output, output.contains("existing_user2"));
        assertFalse(output, output.contains("test_r1"));

        // output should not contain '*' which indicates unknown role
        assertFalse(output, output.contains("*"));
    }

    public void testListUnknownRoles() throws Exception {
        execute(
            "useradd",
            pathHomeParameter,
            fileOrderParameter,
            "username",
            "-p",
            SecuritySettingsSourceField.TEST_PASSWORD,
            "-r",
            "test_r1,r2,r3"
        );
        String output = execute("list", pathHomeParameter, fileOrderParameter, "username");
        assertTrue(output, output.contains("username"));
        assertTrue(output, output.contains("r2*,r3*,test_r1"));
    }

    public void testListNoUsers() throws Exception {
        Files.delete(confDir.resolve("users"));
        Files.createFile(confDir.resolve("users"));
        Files.delete(confDir.resolve("users_roles"));
        Files.createFile(confDir.resolve("users_roles"));
        String output = execute("list", pathHomeParameter, fileOrderParameter);
        assertTrue(output, output.contains("No users found"));
    }

    public void testListUserWithoutRoles() throws Exception {
        String output = execute("list", pathHomeParameter, fileOrderParameter, "existing_user3");
        assertTrue(output, output.contains("existing_user3"));
        output = execute("list", pathHomeParameter, fileOrderParameter);
        assertTrue(output, output.contains("existing_user3"));

        // output should not contain '*' which indicates unknown role
        assertFalse(output, output.contains("*"));
    }

    public void testUserAddNoConfig() throws Exception {
        Path homeDir = jimfs.getPath("eshome");
        IOUtils.rm(confDir.resolve("users"));
        pathHomeParameter = "-Epath.home=" + homeDir;
        fileOrderParameter = "-Expack.security.authc.realms.file.file.order=0";
        UserException e = expectThrows(
            UserException.class,
            () -> {
                execute("useradd", pathHomeParameter, fileOrderParameter, "username", "-p", SecuritySettingsSourceField.TEST_PASSWORD);
            }
        );
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertThat(e.getMessage(), containsString("Configuration file [/work/eshome/config/users] is missing"));
    }

    public void testUserListNoConfig() throws Exception {
        Path homeDir = jimfs.getPath("eshome");
        IOUtils.rm(confDir.resolve("users"));
        pathHomeParameter = "-Epath.home=" + homeDir;
        fileOrderParameter = "-Expack.security.authc.realms.file.file.order=0";
        UserException e = expectThrows(UserException.class, () -> { execute("list", pathHomeParameter, fileOrderParameter); });
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertThat(e.getMessage(), containsString("Configuration file [/work/eshome/config/users] is missing"));
    }

    public void testUserDelNoConfig() throws Exception {
        Path homeDir = jimfs.getPath("eshome");
        IOUtils.rm(confDir.resolve("users"));
        pathHomeParameter = "-Epath.home=" + homeDir;
        fileOrderParameter = "-Expack.security.authc.realms.file.file.order=0";
        UserException e = expectThrows(
            UserException.class,
            () -> { execute("userdel", pathHomeParameter, fileOrderParameter, "username"); }
        );
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertThat(e.getMessage(), containsString("Configuration file [/work/eshome/config/users] is missing"));
    }

    public void testListUserRolesNoConfig() throws Exception {
        Path homeDir = jimfs.getPath("eshome");
        IOUtils.rm(confDir.resolve("users_roles"));
        pathHomeParameter = "-Epath.home=" + homeDir;
        fileOrderParameter = "-Expack.security.authc.realms.file.file.order=0";
        UserException e = expectThrows(UserException.class, () -> { execute("roles", pathHomeParameter, fileOrderParameter, "username"); });
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertThat(e.getMessage(), containsString("Configuration file [/work/eshome/config/users_roles] is missing"));
    }
}
