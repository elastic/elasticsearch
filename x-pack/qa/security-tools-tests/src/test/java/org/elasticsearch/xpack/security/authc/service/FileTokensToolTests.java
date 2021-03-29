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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.service.FileTokensTool.CreateFileTokenCommand;
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
            "elastic/fleet/server_1:" + new String(hasher.hash(token1)),
            "elastic/fleet/server_2:" + new String(hasher.hash(token2)),
            "elastic/fleet/server_3:" + new String(hasher.hash(token3))
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
        };
    }

    public void testParsePrincipalAndTokenName() throws UserException {
        final String tokenName1 = randomAlphaOfLengthBetween(3, 8);
        final Tuple<String, String> tuple1 =
            CreateFileTokenCommand.parsePrincipalAndTokenName(List.of("elastic/fleet", tokenName1), Settings.EMPTY);
        assertEquals("elastic/fleet", tuple1.v1());
        assertEquals(tokenName1, tuple1.v2());

        final UserException e2 = expectThrows(UserException.class,
            () -> CreateFileTokenCommand.parsePrincipalAndTokenName(List.of(randomAlphaOfLengthBetween(6, 16)), Settings.EMPTY));
        assertThat(e2.getMessage(), containsString("Missing token-name argument"));

        final UserException e3 = expectThrows(UserException.class,
            () -> CreateFileTokenCommand.parsePrincipalAndTokenName(List.of(), Settings.EMPTY));
        assertThat(e3.getMessage(), containsString("Missing service-account-principal and token-name arguments"));

        final UserException e4 = expectThrows(UserException.class,
            () -> CreateFileTokenCommand.parsePrincipalAndTokenName(
                List.of(randomAlphaOfLengthBetween(6, 16), randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)),
                Settings.EMPTY));
        assertThat(e4.getMessage(), containsString(
            "Expected two arguments, service-account-principal and token-name, found extra:"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/70959")
    public void testCreateToken() throws Exception {
        final String tokenName1 = ServiceAccountTokenTests.randomTokenName();
        execute("create", pathHomeParameter, "elastic/fleet", tokenName1);
        assertServiceTokenExists("elastic/fleet/" + tokenName1);
        final String tokenName2 = ServiceAccountTokenTests.randomTokenName();
        execute("create", pathHomeParameter, "elastic/fleet", tokenName2);
        assertServiceTokenExists("elastic/fleet/" + tokenName2);
        final String output = terminal.getOutput();
        assertThat(output, containsString("SERVICE_TOKEN elastic/fleet/" + tokenName1 + " = "));
        assertThat(output, containsString("SERVICE_TOKEN elastic/fleet/" + tokenName2 + " = "));
    }

    public void testCreateTokenWithInvalidTokenName() throws Exception {
        final String tokenName = ServiceAccountTokenTests.randomInvalidTokenName();
        final UserException e = expectThrows(UserException.class,
            () -> execute("create", pathHomeParameter, "elastic/fleet", tokenName));
        assertServiceTokenNotExists("elastic/fleet/" + tokenName);
        assertThat(e.getMessage(), containsString(ServiceAccountToken.INVALID_TOKEN_NAME_MESSAGE));
    }

    public void testCreateTokenWithInvalidServiceAccount() throws Exception {
        final UserException e = expectThrows(UserException.class,
            () -> execute("create", pathHomeParameter,
                randomFrom("elastic/foo", "foo/fleet", randomAlphaOfLengthBetween(6, 16)),
                randomAlphaOfLengthBetween(3, 8)));
        assertThat(e.getMessage(), containsString("Unknown service account principal: "));
        assertThat(e.getMessage(), containsString("Must be one of "));
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
