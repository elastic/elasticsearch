/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken.ServiceAccountTokenId;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;

public class FileTokensToolTests extends CommandTestCase {

    // the mock filesystem we use so permissions/users/groups can be modified
    static FileSystem jimfs;
    String pathHomeParameter;

    // the config dir for each test to use
    Path confDir;

    // settings used to create an Environment for tools
    Settings settings;

    Hasher hasher;
    private final SecureString token1 = UUIDs.randomBase64UUIDSecureString();
    private final SecureString token2 = UUIDs.randomBase64UUIDSecureString();
    private final SecureString token3 = UUIDs.randomBase64UUIDSecureString();

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

        Files.write(confDir.resolve("service_tokens"), List.of(
            "elastic/fleet-server/server_1:" + new String(hasher.hash(token1)),
            "elastic/fleet-server/server_2:" + new String(hasher.hash(token2)),
            "elastic/fleet-server/server_3:" + new String(hasher.hash(token3))
        ));
        settings = Settings.builder()
            .put("path.home", homeDir)
            .put("xpack.security.authc.service_token_hashing.algorithm", hasher.name())
            .build();
        pathHomeParameter = "-Epath.home=" + homeDir;
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
        return new FileTokensTool() {
            @Override
            protected CreateFileTokenCommand newCreateFileTokenCommand() {
                return new CreateFileTokenCommand() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        return new Environment(FileTokensToolTests.this.settings, confDir);
                    }
                };
            }

            @Override
            protected DeleteFileTokenCommand newDeleteFileTokenCommand() {
                return new DeleteFileTokenCommand() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        return new Environment(FileTokensToolTests.this.settings, confDir);
                    }
                };
            }

            @Override
            protected ListFileTokenCommand newListFileTokenCommand() {
                return new ListFileTokenCommand() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        return new Environment(FileTokensToolTests.this.settings, confDir);
                    }
                };
            }
        };
    }

    public void testParsePrincipalAndTokenName() throws UserException {
        final String tokenName1 = randomAlphaOfLengthBetween(3, 8);

        final ServiceAccountTokenId accountTokenId =
            FileTokensTool.parsePrincipalAndTokenName(List.of("elastic/fleet-server", tokenName1), Settings.EMPTY);
        assertEquals("elastic/fleet-server", accountTokenId.getAccountId().asPrincipal());
        assertEquals(tokenName1, accountTokenId.getTokenName());

        final UserException e2 = expectThrows(UserException.class,
            () -> FileTokensTool.parsePrincipalAndTokenName(List.of(randomAlphaOfLengthBetween(6, 16)), Settings.EMPTY));
        assertThat(e2.getMessage(), containsString("Missing token-name argument"));

        final UserException e3 = expectThrows(UserException.class,
            () -> FileTokensTool.parsePrincipalAndTokenName(List.of(), Settings.EMPTY));
        assertThat(e3.getMessage(), containsString("Missing service-account-principal and token-name arguments"));

        final UserException e4 = expectThrows(UserException.class,
            () -> FileTokensTool.parsePrincipalAndTokenName(
                List.of(randomAlphaOfLengthBetween(6, 16), randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)),
                Settings.EMPTY));
        assertThat(e4.getMessage(), containsString(
            "Expected two arguments, service-account-principal and token-name, found extra:"));
    }

    public void testCreateToken() throws Exception {
        final String tokenName1 = randomValueOtherThanMany(n -> n.startsWith("-"), ValidationTests::randomTokenName);
        execute("create", pathHomeParameter, "elastic/fleet-server", tokenName1);
        assertServiceTokenExists("elastic/fleet-server/" + tokenName1);
        final String tokenName2 = randomValueOtherThanMany(n -> n.startsWith("-") || n.equals(tokenName1),
            ValidationTests::randomTokenName);
        execute("create", pathHomeParameter, "elastic/fleet-server", tokenName2);
        assertServiceTokenExists("elastic/fleet-server/" + tokenName2);
        // token name with a leading hyphen requires an option terminator
        final String tokenName3 = "-" + ValidationTests.randomTokenName().substring(1);
        execute("create", pathHomeParameter, "elastic/fleet-server", "--", tokenName3);
        assertServiceTokenExists("elastic/fleet-server/" + tokenName3);
        final String output = terminal.getOutput();
        assertThat(output, containsString("SERVICE_TOKEN elastic/fleet-server/" + tokenName1 + " = "));
        assertThat(output, containsString("SERVICE_TOKEN elastic/fleet-server/" + tokenName2 + " = "));
        assertThat(output, containsString("SERVICE_TOKEN elastic/fleet-server/" + tokenName3 + " = "));
    }

    public void testCreateTokenWithInvalidTokenName() throws Exception {
        final String tokenName = ValidationTests.randomInvalidTokenName();
        final String[] args = tokenName.startsWith("-") ?
            new String[] { "create", pathHomeParameter, "elastic/fleet-server", "--", tokenName } :
            new String[] { "create", pathHomeParameter, "elastic/fleet-server", tokenName };
        final UserException e = expectThrows(UserException.class, () -> execute(args));
        assertServiceTokenNotExists("elastic/fleet-server/" + tokenName);
        assertThat(e.getMessage(), containsString(Validation.INVALID_SERVICE_ACCOUNT_TOKEN_NAME_MESSAGE));
        assertThat(e.getMessage(), containsString("invalid service token name [" + tokenName + "]"));
    }

    public void testCreateTokenWithInvalidServiceAccount() throws Exception {
        final UserException e = expectThrows(UserException.class,
            () -> execute("create", pathHomeParameter,
                randomFrom("elastic/foo", "foo/fleet-server", randomAlphaOfLengthBetween(6, 16)),
                randomAlphaOfLengthBetween(3, 8)));
        assertThat(e.getMessage(), containsString("Unknown service account principal: "));
        assertThat(e.getMessage(), containsString("Must be one of "));
    }

    public void testDeleteToken() throws Exception {
        final String tokenName = randomFrom("server_1", "server_2", "server_3");
        final String principal = "elastic/fleet-server";
        final String qualifiedName = principal + "/" + tokenName;
        assertServiceTokenExists(qualifiedName);
        execute("delete", pathHomeParameter, principal, tokenName);
        assertServiceTokenNotExists(qualifiedName);
    }

    public void testDeleteTokenIncorrect() throws IOException {
        // Invalid principal
        final UserException e1 = expectThrows(UserException.class,
            () -> execute("delete", pathHomeParameter,
                randomFrom("elastic/foo", "foo/fleet-server", randomAlphaOfLengthBetween(6, 16)),
                randomAlphaOfLengthBetween(3, 8)));
        assertThat(e1.getMessage(), containsString("Unknown service account principal: "));
        assertThat(e1.getMessage(), containsString("Must be one of "));

        // Invalid token name
        final String tokenName2 = ValidationTests.randomInvalidTokenName();
        final String[] args = tokenName2.startsWith("-") ?
            new String[] { "delete", pathHomeParameter, "elastic/fleet-server", "--", tokenName2 } :
            new String[] { "delete", pathHomeParameter, "elastic/fleet-server", tokenName2 };
        final UserException e2 = expectThrows(UserException.class, () -> execute(args));
        assertThat(e2.getMessage(), containsString(Validation.INVALID_SERVICE_ACCOUNT_TOKEN_NAME_MESSAGE));
        assertThat(e2.getMessage(), containsString("invalid service token name [" + tokenName2 + "]"));

        // Non-exist token
        final Path serviceTokensFile = confDir.resolve("service_tokens");
        final boolean fileDeleted = randomBoolean();
        if (fileDeleted) {
            Files.delete(serviceTokensFile);
        }
        final String tokenName3 = randomAlphaOfLengthBetween(3, 8);
        final UserException e3 = expectThrows(UserException.class,
            () -> execute("delete", pathHomeParameter, "elastic/fleet-server", tokenName3));
        assertThat(e3.getMessage(), containsString("Service token [elastic/fleet-server/" + tokenName3 + "] does not exist"));
        if (fileDeleted) {
            // The file should not be created if not exists in the first place
            assertThat(Files.notExists(serviceTokensFile), is(true));
        }
    }

    public void testListTokens() throws Exception {
        execute("list", pathHomeParameter);
        final String output = terminal.getOutput();
        assertThat(output, containsString("elastic/fleet-server/server_1\n" +
            "elastic/fleet-server/server_2\n" +
            "elastic/fleet-server/server_3"));
    }

    public void testListTokensByPrincipal() throws Exception {
        execute("list", pathHomeParameter, "elastic/fleet-server");
        final String output = terminal.getOutput();
        assertThat(output, containsString("elastic/fleet-server/server_1\n" +
            "elastic/fleet-server/server_2\n" +
            "elastic/fleet-server/server_3"));
    }

    public void testListTokensNonExist() throws Exception {
        // Invalid principal
        final UserException e1 = expectThrows(UserException.class,
            () -> execute("list", pathHomeParameter,
                randomFrom("elastic/foo", "foo/fleet-server", randomAlphaOfLengthBetween(6, 16))));
        assertThat(e1.getMessage(), containsString("Unknown service account principal: "));
        assertThat(e1.getMessage(), containsString("Must be one of "));

        // Delete all tokens
        execute("delete", pathHomeParameter, "elastic/fleet-server", "server_1");
        execute("delete", pathHomeParameter, "elastic/fleet-server", "server_2");
        execute("delete", pathHomeParameter, "elastic/fleet-server", "server_3");
        execute("list", pathHomeParameter, "elastic/fleet-server");
        final String output = terminal.getOutput();
        assertThat(output, is(emptyString()));
    }

    public void testListTokensFileNotExists() throws Exception {
        final Path serviceTokensFile = confDir.resolve("service_tokens");
        Files.delete(serviceTokensFile);
        execute("list", pathHomeParameter, "elastic/fleet-server");
        final String output = terminal.getOutput();
        assertThat(output, is(emptyString()));
        // List should not create the file
        assertThat(Files.notExists(serviceTokensFile), is(true));
    }

    private void assertServiceTokenExists(String key) throws IOException {
        List<String> lines = Files.readAllLines(confDir.resolve("service_tokens"), StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] keyHash = line.split(":", 2);
            if (keyHash.length != 2) {
                fail("Corrupted service_tokens file, line: " + line);
            }
            if (key.equals(keyHash[0])) {
                return;
            }
        }
        fail("Could not find key " + key + " in service_tokens file:\n" + lines.toString());
    }

    private void assertServiceTokenNotExists(String key) throws IOException {
        List<String> lines = Files.readAllLines(confDir.resolve("service_tokens"), StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] keyHash = line.split(":", 2);
            if (keyHash.length != 2) {
                fail("Corrupted service_tokens file, line: " + line);
            }
            assertNotEquals(key, keyHash[0]);
        }
    }
}
