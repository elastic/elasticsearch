/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment.tool;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import joptsimple.OptionSet;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.tool.BaseRunAsSuperuserCommand;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class BaseRunAsSuperuserCommandTests extends CommandTestCase {

    private CommandLineHttpClient client;
    private KeyStoreWrapper keyStoreWrapper;
    private static Hasher hasher;
    static FileSystem jimfs;
    private Path confDir;
    private Settings settings;

    @Override
    protected Command newCommand() {
        return new DummyRunAsSuperuserCommand(environment -> client, environment -> keyStoreWrapper) {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return new Environment(BaseRunAsSuperuserCommandTests.this.settings, confDir);
            }
        };
    }

    @BeforeClass
    public static void setupJimfs() {
        String view = randomFrom("basic", "posix");
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews(view).build();
        jimfs = Jimfs.newFileSystem(conf);
        PathUtilsForTesting.installMock(jimfs);
    }

    @Before
    public void setup() throws Exception {
        Path homeDir = jimfs.getPath("eshome");
        IOUtils.rm(homeDir);
        confDir = homeDir.resolve("config");
        Files.createDirectories(confDir);
        Files.write(confDir.resolve("users"), List.of(), StandardCharsets.UTF_8);
        Files.write(confDir.resolve("users_roles"), List.of(), StandardCharsets.UTF_8);
        hasher = getFastStoredHashAlgoForTests();
        settings = Settings.builder()
            .put("path.home", homeDir)
            .put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), hasher.name())
            .build();

        this.client = mock(CommandLineHttpClient.class);
        when(client.getDefaultURL()).thenReturn("https://localhost:9200");

        URL url = new URL(client.getDefaultURL());
        HttpResponse healthResponse = new HttpResponse(
            HttpURLConnection.HTTP_OK,
            Map.of("status", randomFrom("yellow", "green"))
        );
        when(
            client.execute(
                anyString(),
                eq(clusterHealthUrl(url)),
                anyString(),
                any(SecureString.class),
                any(CheckedSupplier.class),
                any(CheckedFunction.class)
            )
        ).thenReturn(healthResponse);
    }

    @AfterClass
    public static void closeJimfs() throws IOException {
        if (jimfs != null) {
            jimfs.close();
            jimfs = null;
        }
    }

    public void testSuccessfulCommand() throws Exception {
        execute();
        assertThat(terminal.getOutput(), is(emptyString()));
        assertThat(terminal.getErrorOutput(), is(emptyString()));
        assertNoUsers();
        assertNoUsersRoles();
    }

    public void testFailureWhenFileRealmIsDisabled() throws Exception {
        settings = Settings.builder()
            .put(settings)
            .put("xpack.security.authc.realms.file." + randomAlphaOfLength(8) + ".enabled", false)
            .build();
        UserException e = expectThrows(UserException.class, this::execute);
        assertThat(e.getMessage(), equalTo("File realm must be enabled"));
        assertThat(terminal.getOutput(), is(emptyString()));
        assertNoUsers();
        assertNoUsersRoles();
    }

    public void testUsersFileIsMissing() throws Exception {
        final Path usersPath = confDir.resolve("users");
        Files.delete(usersPath);
        UserException e = expectThrows(UserException.class, this::execute);
        assertThat(e.getMessage(), equalTo("File realm configuration file [/work/" + usersPath + "] is missing"));
        assertThat(terminal.getOutput(), is(emptyString()));
    }

    public void testUsersRolesFileIsMissing() throws Exception {
        final Path rolesPath = confDir.resolve("users_roles");
        Files.delete(rolesPath);
        UserException e = expectThrows(UserException.class, this::execute);
        assertThat(e.getMessage(),
            equalTo("File realm configuration file [/work/" + rolesPath + "] is missing"));
        assertThat(terminal.getOutput(), is(emptyString()));
    }

    public void testUnhealthyCluster() throws Exception {
        doThrow(new IllegalStateException("Failed to determine the health of the cluster. Cluster health is currently RED."))
            .when(client).checkClusterHealthWithRetriesWaitingForCluster(anyString(), any(SecureString.class), anyInt(), anyBoolean());
        UserException e = expectThrows(UserException.class, this::execute);
        assertThat(e.exitCode, equalTo(ExitCodes.UNAVAILABLE));
        assertThat(e.getMessage(), containsString("RED"));
        assertThat(terminal.getOutput(), is(emptyString()));
        String error = terminal.getErrorOutput();
        assertThat(error, stringContainsInOrder("Failed to determine the health of the cluster. Cluster health is currently RED.",
            "This means that some cluster data is unavailable and your cluster is not fully functional.",
            "The cluster logs (https://www.elastic.co/guide/en/elasticsearch/reference/"
                + Version.CURRENT.major + "." + Version.CURRENT.minor + "/logging.html)" +
                " might contain information/indications for the underlying cause"));
        assertNoUsers();
        assertNoUsersRoles();
    }

    public void testUnhealthyClusterWithForce() throws Exception {
        URL url = new URL(client.getDefaultURL());
        HttpResponse healthResponse =
            new HttpResponse(HttpURLConnection.HTTP_OK, Map.of("status", randomFrom("red")));
        when(client.execute(anyString(), eq(clusterHealthUrl(url)), anyString(), any(SecureString.class), any(CheckedSupplier.class),
            any(CheckedFunction.class))).thenReturn(healthResponse);
        execute("-f");
        assertThat(terminal.getOutput(), is(emptyString()));
        assertThat(terminal.getErrorOutput(), is(emptyString()));
        assertNoUsers();
        assertNoUsersRoles();
    }

    public void testWillRetryOnUnauthorized() throws Exception {
        doThrow(new ElasticsearchStatusException(
            "Failed to determine the health of the cluster. Unexpected http status [401]", RestStatus.fromCode(401)))
            .when(client).checkClusterHealthWithRetriesWaitingForCluster(anyString(), any(SecureString.class), anyInt(), anyBoolean());
        UserException e = expectThrows(UserException.class, () -> execute("--verbose"));
        String verboseOutput = terminal.getOutput();
        assertThat(verboseOutput.split("\\n").length, equalTo(5));
        assertThat(verboseOutput,
            containsString("Unexpected http status [401] while attempting to determine cluster health. Will retry at most"));
        assertThat(e.exitCode, equalTo(ExitCodes.DATA_ERROR));
        assertNoUsers();
        assertNoUsersRoles();
    }

    public void testWithPasswordProtectedKeystore() throws Exception {
        this.keyStoreWrapper = mock(KeyStoreWrapper.class);
        when(keyStoreWrapper.isLoaded()).thenReturn(true);
        when(keyStoreWrapper.hasPassword()).thenReturn(true);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                char[] password = (char[]) args[0];
                if (Arrays.equals(password, "keystore-password".toCharArray()) == false) {
                    throw new GeneralSecurityException("Wrong password");
                }
                return null;
            }
        }).when(keyStoreWrapper).decrypt(any());
        terminal.addSecretInput("some-other-password");
        UserException e = expectThrows(UserException.class, this::execute);
        assertThat(e.exitCode, equalTo(ExitCodes.DATA_ERROR));
        assertNoUsers();
        assertNoUsersRoles();
        assertThat(terminal.getOutput(), is(emptyString()));
        terminal.addSecretInput("keystore-password");
        execute();
        assertThat(terminal.getOutput(), is(emptyString()));
        assertNoUsers();
        assertNoUsersRoles();
    }

    private void assertNoUsers() throws Exception {
        List<String> lines = Files.readAllLines(confDir.resolve("users"), StandardCharsets.UTF_8);
        assertThat(lines.size(), equalTo(0));
    }

    private void assertNoUsersRoles() throws Exception {
        List<String> lines = Files.readAllLines(confDir.resolve("users_roles"), StandardCharsets.UTF_8);
        assertThat(lines.size(), equalTo(0));
    }

    private URL clusterHealthUrl(URL url) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_cluster/health").replaceAll("/+", "/") + "?pretty");
    }

    /**
     * {@link DummyRunAsSuperuserCommand#executeCommand(Terminal, OptionSet, Environment, String, SecureString)} is executed while the file
     * realm user is persisted in file and still valid. We check that the username and the password that would be passed to extending
     * Classes as parameters are what is actually created and stored in the file realm.
     */
    static class DummyRunAsSuperuserCommand extends BaseRunAsSuperuserCommand {
        DummyRunAsSuperuserCommand(
            Function<Environment, CommandLineHttpClient> clientFunction,
            CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction
        ) {
            super(clientFunction, keyStoreFunction, "dummy command");
        }

        @Override
        protected void executeCommand(Terminal terminal, OptionSet options, Environment env, String username, SecureString password)
            throws Exception {
            final Path confDir = jimfs.getPath(env.settings().get("path.home")).resolve("config");
            List<String> lines = Files.readAllLines(confDir.resolve("users"), StandardCharsets.UTF_8);
            assertThat(lines.size(), equalTo(1));
            assertThat(lines.get(0), containsString(username));
            final char[] passwordHashFromFile = lines.get(0).split(":")[1].toCharArray();
            hasher.verify(password, passwordHashFromFile);
        }

        @Override
        protected void validate(Terminal terminal, OptionSet options, Environment env) throws Exception {

        }
    }
}
