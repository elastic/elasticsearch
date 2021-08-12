/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment.tool;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.enrollment.EnrollmentToken;
import org.elasticsearch.xpack.security.enrollment.EnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class CreateEnrollmentTokenToolTests extends CommandTestCase {

    static FileSystem jimfs;
    String pathHomeParameter;
    Path confDir;
    Settings settings;

    private CommandLineHttpClient client;
    private KeyStoreWrapper keyStoreWrapper;
    private EnrollmentTokenGenerator enrollmentTokenGenerator;

    @Override
    protected Command newCommand() {
        return new CreateEnrollmentTokenTool(environment -> client, environment -> keyStoreWrapper,
            environment -> enrollmentTokenGenerator) {
            @Override
            protected Environment createEnv(Map<String, String> settings) {
                return new Environment(CreateEnrollmentTokenToolTests.this.settings, confDir);
            }
        };
    }

    @BeforeClass
    public static void muteInFips(){
        assumeFalse("Enrollment mode is not supported in FIPS mode.", inFipsJvm());
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
        Files.write(confDir.resolve("users_roles"),  List.of(), StandardCharsets.UTF_8);
        settings = Settings.builder()
            .put("path.home", homeDir)
            .put("xpack.security.enrollment.enabled", true)
            .build();
        pathHomeParameter = "-Epath.home=" + homeDir;

        this.keyStoreWrapper = mock(KeyStoreWrapper.class);
        when(keyStoreWrapper.isLoaded()).thenReturn(true);

        this.client = mock(CommandLineHttpClient.class);
        when(client.getDefaultURL()).thenReturn("https://localhost:9200");

        URL url = new URL(client.getDefaultURL());
        HttpResponse healthResponse =
            new HttpResponse(HttpURLConnection.HTTP_OK, Map.of("status", randomFrom("yellow", "green")));
        when(client.execute(anyString(), eq(clusterHealthUrl(url)), anyString(), any(SecureString.class), any(CheckedSupplier.class),
            any(CheckedFunction.class))).thenReturn(healthResponse);

        this.enrollmentTokenGenerator = mock(EnrollmentTokenGenerator.class);
        EnrollmentToken kibanaToken = new EnrollmentToken("DR6CzXkBDf8amV_48yYX:x3YqU_rqQwm-ESrkExcnOg",
            "ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", "8.0.0",
            Arrays.asList("[192.168.0.1:9201, 172.16.254.1:9202"));
        EnrollmentToken nodeToken = new EnrollmentToken("DR6CzXkBDf8amV_48yYX:4BhUk-mkFm-AwvRFg90KJ",
            "ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", "8.0.0",
            Arrays.asList("[192.168.0.1:9201, 172.16.254.1:9202"));
        when(enrollmentTokenGenerator.createKibanaEnrollmentToken(anyString(), any(SecureString.class)))
            .thenReturn(kibanaToken);
        when(enrollmentTokenGenerator.createNodeEnrollmentToken(anyString(), any(SecureString.class)))
            .thenReturn(nodeToken);
    }

    @AfterClass
    public static void closeJimfs() throws IOException {
        if (jimfs != null) {
            jimfs.close();
            jimfs = null;
        }
    }

    public void testCreateToken() throws Exception {
        String scope = randomBoolean() ? "node" : "kibana";
        String output = execute("--scope", scope);
        if (scope.equals("kibana")) {
            assertThat(output, containsString("1WXzQ4eVlYOngzWXFVX3JxUXdtLUVTcmtFeGNuT2cifQ=="));
        } else {
            assertThat(output, containsString("4YW1WXzQ4eVlYOjRCaFVrLW1rRm0tQXd2UkZnOTBLSiJ9"));
        }
    }

    public void testInvalidScope() throws Exception {
        String scope = randomAlphaOfLength(14);
        UserException e = expectThrows(UserException.class, () -> {
            execute(randomFrom("-s", "--s"), scope);
        });
        assertThat(e.exitCode, equalTo(ExitCodes.USAGE));
        assertThat(e.getMessage(), equalTo("Invalid scope"));
        assertThat(terminal.getErrorOutput(),
            containsString("The scope of this enrollment token, can only be one of "+ CreateEnrollmentTokenTool.ALLOWED_SCOPES));
    }

    public void testUnhealthyCluster() throws Exception {
        String scope = randomBoolean() ? "node" : "kibana";
        doThrow(new IllegalStateException("Failed to determine the health of the cluster. Cluster health is currently RED."))
            .when(client).checkClusterHealthWithRetriesWaitingForCluster(anyString(), any(SecureString.class), anyInt());
        UserException e = expectThrows(UserException.class, () -> {
            execute(randomFrom("-s", "--s"), scope);
        });
        assertThat(e.exitCode, equalTo(ExitCodes.UNAVAILABLE));
        assertThat(e.getMessage(), containsString("RED"));
    }

    public void testUnhealthyClusterWithForce() throws Exception {
        String scope = randomBoolean() ? "node" : "kibana";
        String output = execute("--scope", scope);
        if (scope.equals("kibana")) {
            assertThat(output, containsString("1WXzQ4eVlYOngzWXFVX3JxUXdtLUVTcmtFeGNuT2cifQ=="));
        } else {
            assertThat(output, containsString("4YW1WXzQ4eVlYOjRCaFVrLW1rRm0tQXd2UkZnOTBLSiJ9"));
        }
    }

    public void testEnrollmentDisabled() {
        settings = Settings.builder()
            .put(settings)
            .put(XPackSettings.ENROLLMENT_ENABLED.getKey(), false)
            .build();

        String scope = randomBoolean() ? "node" : "kibana";
        UserException e = expectThrows(UserException.class, () -> {
            execute(randomFrom("-s", "--s"), scope);
        });
        assertThat(e.exitCode, equalTo(ExitCodes.CONFIG));
        assertThat(e.getMessage(),
            equalTo("[xpack.security.enrollment.enabled] must be set to `true` to create an enrollment token"));
    }

    public void testUnableToCreateToken() throws Exception {
        this.enrollmentTokenGenerator = mock(EnrollmentTokenGenerator.class);
        when(enrollmentTokenGenerator.createKibanaEnrollmentToken(anyString(), any(SecureString.class)))
            .thenThrow(new IllegalStateException("example exception message"));
        when(enrollmentTokenGenerator.createNodeEnrollmentToken(anyString(), any(SecureString.class)))
            .thenThrow(new IllegalStateException("example exception message"));
        String scope = randomBoolean() ? "node" : "kibana";
        UserException e = expectThrows(UserException.class, () -> {
            execute(randomFrom("-s", "--s"), scope);
        });
        assertThat(e.exitCode, equalTo(ExitCodes.CANT_CREATE));
        assertThat(e.getMessage(),
            equalTo("example exception message"));
    }

    private URL clusterHealthUrl(URL url) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_cluster/health").replaceAll("/+", "/") + "?pretty");
    }
}
