/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.esnative.tool;

import joptsimple.OptionSet;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient;
import org.elasticsearch.xpack.core.security.HttpResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class ResetPasswordToolTests extends CommandTestCase {
    static FileSystem jimfs;
    String pathHomeParameter;
    Path confDir;
    Settings settings;

    private CommandLineHttpClient client;
    private KeyStoreWrapper keyStoreWrapper;
    private String user;

    @Override
    protected Command newCommand() {
        return new ResetPasswordTool(environment -> client, environment -> keyStoreWrapper) {
            @Override
            protected Environment createEnv(OptionSet options, ProcessInfo processInfo) throws UserException {
                return new Environment(ResetPasswordToolTests.this.settings, confDir);
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
        settings = Settings.builder().put("path.home", homeDir).put("xpack.security.enrollment.enabled", true).build();
        pathHomeParameter = "-Epath.home=" + homeDir;

        this.keyStoreWrapper = mock(KeyStoreWrapper.class);
        when(keyStoreWrapper.isLoaded()).thenReturn(true);

        this.client = mock(CommandLineHttpClient.class);
        when(client.getDefaultURL()).thenReturn("https://localhost:9200");
        user = randomFrom("elastic", "kibana_system", "nativeuser1");
        URL url = new URL(client.getDefaultURL());
        HttpResponse healthResponse = new HttpResponse(HttpURLConnection.HTTP_OK, Map.of("status", randomFrom("yellow", "green")));
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
        HttpResponse changePasswordResponse = new HttpResponse(HttpURLConnection.HTTP_OK, Map.of());
        when(
            client.execute(
                anyString(),
                eq(changePasswordUrl(url, user)),
                anyString(),
                any(SecureString.class),
                any(CheckedSupplier.class),
                any(CheckedFunction.class)
            )
        ).thenReturn(changePasswordResponse);
    }

    @AfterClass
    public static void closeJimfs() throws IOException {
        if (jimfs != null) {
            jimfs.close();
            jimfs = null;
        }
    }

    public void testSuccessAutoMode() throws Exception {
        terminal.addTextInput("y");
        execute(randomFrom("-u", "--username"), user);
        String output = terminal.getOutput();
        assertThat(output, containsString("This tool will reset the password of the [" + user + "] user to an autogenerated value."));
        assertThat(output, containsString("The password will be printed in the console."));
        assertThat(output, containsString("Password for the [" + user + "] user successfully reset."));
        assertThat(output, containsString("New value:"));
    }

    public void testSuccessInteractiveMode() throws Exception {
        final String password = randomAlphaOfLengthBetween(6, 18);
        terminal.addTextInput("y");
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        execute(randomFrom("-i", "--interactive"), randomFrom("-u", "--username"), user);
        String output = terminal.getOutput();
        assertThat(output, containsString("This tool will reset the password of the [" + user + "] user."));
        assertThat(output, containsString("You will be prompted to enter the password."));
        assertThat(output, containsString("Password for the [" + user + "] user successfully reset."));
    }

    public void testUserCanPassUrlParameter() throws Exception {
        URL url = new URL("http://localhost:9204");
        HttpResponse healthResponse = new HttpResponse(HttpURLConnection.HTTP_OK, Map.of("status", randomFrom("yellow", "green")));
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
        HttpResponse changePasswordResponse = new HttpResponse(HttpURLConnection.HTTP_OK, Map.of());
        when(
            client.execute(
                anyString(),
                eq(changePasswordUrl(url, user)),
                anyString(),
                any(SecureString.class),
                any(CheckedSupplier.class),
                any(CheckedFunction.class)
            )
        ).thenReturn(changePasswordResponse);
        terminal.addTextInput("y");
        execute(randomFrom("-u", "--username"), user, "--url", "http://localhost:9204");
        String output = terminal.getOutput();
        assertThat(output, containsString("This tool will reset the password of the [" + user + "] user to an autogenerated value."));
        assertThat(output, containsString("The password will be printed in the console."));
        assertThat(output, containsString("Password for the [" + user + "] user successfully reset."));
        assertThat(output, containsString("New value:"));
    }

    public void testUserCancelledAutoMode() throws Exception {
        terminal.addTextInput("n");
        UserException e = expectThrows(UserException.class, () -> execute(randomFrom("-u", "--username"), user));
        assertThat(e.getMessage(), equalTo("User cancelled operation"));
        String output = terminal.getOutput();
        assertThat(output, containsString("This tool will reset the password of the [" + user + "] user to an autogenerated value."));
        assertThat(output, containsString("The password will be printed in the console."));
    }

    public void testFailureInteractiveModeDifferentPassword() throws Exception {
        final String password1 = randomAlphaOfLengthBetween(6, 18);
        final String password2 = randomAlphaOfLengthBetween(6, 18);
        terminal.addTextInput("y");
        terminal.addSecretInput(password1);
        terminal.addSecretInput(password2);
        terminal.addSecretInput(password1);
        terminal.addSecretInput(password1);
        execute(randomFrom("-i", "--interactive"), randomFrom("-u", "--username"), user);
        String output = terminal.getOutput();
        String error = terminal.getErrorOutput();
        assertThat(output, containsString("This tool will reset the password of the [" + user + "] user."));
        assertThat(output, containsString("You will be prompted to enter the password."));
        assertThat(output, containsString("Password for the [" + user + "] user successfully reset."));
        assertThat(error, containsString("Passwords do not match."));
        assertThat(error, containsString("Try again."));
    }

    public void testFailureClusterUnhealthy() throws Exception {
        final URL url = new URL(client.getDefaultURL());
        HttpResponse healthResponse = new HttpResponse(HttpURLConnection.HTTP_OK, Map.of("status", randomFrom("red")));
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
        UserException e = expectThrows(
            UserException.class,
            () -> { execute(randomFrom("-i", "-a"), randomFrom("-u", "--username"), user); }
        );
        assertThat(e.exitCode, equalTo(ExitCodes.UNAVAILABLE));
        assertThat(e.getMessage(), containsString("RED"));
        assertThat(terminal.getOutput(), is(emptyString()));
    }

    public void testFailureUnableToChangePassword() throws Exception {
        terminal.addTextInput("y");
        final URL url = new URL(client.getDefaultURL());
        HttpResponse changePasswordResponse = new HttpResponse(HttpURLConnection.HTTP_UNAVAILABLE, Map.of());
        when(
            client.execute(
                anyString(),
                eq(changePasswordUrl(url, user)),
                anyString(),
                any(SecureString.class),
                any(CheckedSupplier.class),
                any(CheckedFunction.class)
            )
        ).thenReturn(changePasswordResponse);
        UserException e = expectThrows(UserException.class, () -> execute(randomFrom("-u", "--username"), user));
        assertThat(e.exitCode, equalTo(ExitCodes.TEMP_FAILURE));
        assertThat(e.getMessage(), equalTo("Failed to reset password for the [" + user + "] user"));
        String output = terminal.getOutput();
        assertThat(output, containsString("This tool will reset the password of the [" + user + "] user to an autogenerated value."));
        assertThat(output, containsString("The password will be printed in the console."));
    }

    public void testFailureClusterUnhealthyWithForce() throws Exception {
        terminal.addTextInput("y");
        final URL url = new URL(client.getDefaultURL());
        HttpResponse healthResponse = new HttpResponse(HttpURLConnection.HTTP_OK, Map.of("status", randomFrom("red")));
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
        execute("-a", randomFrom("-f", "--force"), randomFrom("-u", "--username"), user);
        String output = terminal.getOutput();
        assertThat(output, containsString("This tool will reset the password of the [" + user + "] user to an autogenerated value."));
        assertThat(output, containsString("The password will be printed in the console."));
        assertThat(output, containsString("Password for the [" + user + "] user successfully reset."));
        assertThat(output, containsString("New value:"));
    }

    public void testAutoInteractiveModesMutuallyExclusive() throws Exception {
        UserException e = expectThrows(
            UserException.class,
            () -> execute(randomFrom("-i", "--interactive"), randomFrom("-a", "--auto"), randomFrom("-u", "--username"), user)
        );
        assertThat(e.exitCode, equalTo(ExitCodes.USAGE));
        assertThat(e.getMessage(), equalTo("You can only run the tool in one of [auto] or [interactive] modes"));
        assertThat(terminal.getOutput(), is(emptyString()));
    }

    public void testAutoBatchSilent() throws Exception {
        execute(randomFrom("--silent", "-s"), randomFrom("--batch", "-b"), randomFrom("-u", "--username"), user);
        assertThat(terminal.getOutput(), hasLength(20 + System.lineSeparator().length())); // password + new line char
        assertThat(terminal.getErrorOutput(), is(emptyString()));
    }

    public void testInvalidInvocation() throws Exception {
        Exception e = expectThrows(Exception.class, () -> execute(randomFrom("-i", "--interactive")));
        assertThat(e.getMessage(), containsString("Missing required option(s) [u/username]"));
        assertThat(terminal.getOutput(), is(emptyString()));
    }

    private URL changePasswordUrl(URL url, String user) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_security/user/" + user + "/_password").replaceAll("//+", "/") + "?pretty");
    }

    private URL clusterHealthUrl(URL url) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_cluster/health").replaceAll("//+", "/") + "?pretty");
    }
}
