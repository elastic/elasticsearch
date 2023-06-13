/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient;
import org.elasticsearch.xpack.core.security.HttpResponse;
import org.elasticsearch.xpack.core.security.HttpResponse.HttpResponseBuilder;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLException;

import static org.elasticsearch.test.CheckedFunctionUtils.anyCheckedFunction;
import static org.elasticsearch.test.CheckedFunctionUtils.anyCheckedSupplier;
import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetupPasswordToolTests extends CommandTestCase {

    private SecureString bootstrapPassword;
    private CommandLineHttpClient httpClient;
    private List<String> usersInSetOrder;
    private KeyStoreWrapper passwordProtectedKeystore;
    private KeyStoreWrapper keyStore;
    private KeyStoreWrapper usedKeyStore;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setupSecretsAndKeystore() throws Exception {
        resetSecretsAndKeyStore(null);
    }

    public void resetSecretsAndKeyStore(String promptResponse) throws Exception {
        terminal.reset();
        // sometimes we fall back to the keystore seed as this is the default when a new node starts
        boolean useFallback = randomBoolean();
        bootstrapPassword = useFallback
            ? new SecureString("0xCAFEBABE".toCharArray())
            : new SecureString("bootstrap-password".toCharArray());
        keyStore = mockKeystore(false, useFallback);
        // create a password protected keystore eitherway, so that it can be used for SetupPasswordToolTests#testWrongKeystorePassword
        passwordProtectedKeystore = mockKeystore(true, useFallback);
        usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        if (usedKeyStore.hasPassword()) {
            terminal.addSecretInput("keystore-password");
        }
        if (promptResponse != null) {
            terminal.addTextInput(promptResponse);
        }

        this.httpClient = mock(CommandLineHttpClient.class);
        when(httpClient.getDefaultURL()).thenReturn("http://localhost:9200");

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<>());
        when(
            httpClient.execute(
                anyString(),
                any(URL.class),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponse);

        URL url = new URL(httpClient.getDefaultURL());
        httpResponse = new HttpResponse(HttpURLConnection.HTTP_OK, Collections.singletonMap("status", randomFrom("yellow", "green")));
        when(
            httpClient.execute(
                anyString(),
                eq(clusterHealthUrl(url)),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponse);

        URL xpackSecurityPluginQueryURL = queryXPackSecurityFeatureConfigURL(url);
        HttpResponse queryXPackSecurityConfigHttpResponse = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<>());
        when(
            httpClient.execute(
                eq("GET"),
                eq(xpackSecurityPluginQueryURL),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(queryXPackSecurityConfigHttpResponse);

        // elastic user is updated last
        usersInSetOrder = new ArrayList<>(SetupPasswordTool.USERS);
        usersInSetOrder.sort((user1, user2) -> {
            if (SetupPasswordTool.USERS_WITH_SHARED_PASSWORDS.containsKey(user1)
                && SetupPasswordTool.USERS_WITH_SHARED_PASSWORDS.containsValue(user2)) {
                return -1;
            }
            if (SetupPasswordTool.USERS_WITH_SHARED_PASSWORDS.containsKey(user2)
                && SetupPasswordTool.USERS_WITH_SHARED_PASSWORDS.containsValue(user1)) {
                return 1;
            }
            return 0;
        });

        for (int i = 0; i < usersInSetOrder.size() - 1; i++) {
            if (ElasticUser.NAME.equals(usersInSetOrder.get(i))) {
                Collections.swap(usersInSetOrder, i, i + 1);
            }
        }

        for (String user : SetupPasswordTool.USERS) {
            if (SetupPasswordTool.USERS_WITH_SHARED_PASSWORDS.containsValue(user)) {
                continue;
            }
            terminal.addSecretInput(user + "-password");
            terminal.addSecretInput(user + "-password");
        }
    }

    private KeyStoreWrapper mockKeystore(boolean isPasswordProtected, boolean useFallback) throws Exception {
        KeyStoreWrapper keyStore = mock(KeyStoreWrapper.class);
        when(keyStore.isLoaded()).thenReturn(true);
        if (useFallback) {
            when(keyStore.getSettingNames()).thenReturn(
                new HashSet<>(Arrays.asList(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), KeyStoreWrapper.SEED_SETTING.getKey()))
            );
            when(keyStore.getString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey())).thenReturn(bootstrapPassword);
        } else {
            when(keyStore.getSettingNames()).thenReturn(Collections.singleton(KeyStoreWrapper.SEED_SETTING.getKey()));
            when(keyStore.getString(KeyStoreWrapper.SEED_SETTING.getKey())).thenReturn(bootstrapPassword);
        }
        if (isPasswordProtected) {
            when(keyStore.hasPassword()).thenReturn(true);
            doNothing().when(keyStore).decrypt("keystore-password".toCharArray());
            doThrow(new SecurityException("Provided keystore password was incorrect", new IOException())).when(keyStore)
                .decrypt("wrong-password".toCharArray());
        }
        return keyStore;
    }

    @Override
    protected Command newCommand() {
        return getSetupPasswordCommandWithKeyStore(usedKeyStore);
    }

    public void testAutoSetup() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        if (randomBoolean()) {
            execute("auto", "-b", "true");
        } else {
            resetSecretsAndKeyStore("Y");
            execute("auto");
        }
        if (usedKeyStore.hasPassword()) {
            // SecureString is already closed (zero-filled) and keystore-password is 17 char long
            verify(usedKeyStore).decrypt(new char[17]);
        } else {
            verify(usedKeyStore).decrypt(new char[0]);
        }

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = authenticateUrl(url);
        inOrder.verify(httpClient)
            .execute(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), anyCheckedSupplier(), anyCheckedFunction());
        Map<String, String> capturedPasswords = Maps.newMapWithExpectedSize(usersInSetOrder.size());
        for (String user : usersInSetOrder) {
            URL urlWithRoute = passwordUrl(url, user);
            @SuppressWarnings({ "unchecked", "rawtypes" })
            ArgumentCaptor<CheckedSupplier<String, Exception>> passwordCaptor = ArgumentCaptor.forClass((Class) CheckedSupplier.class);
            inOrder.verify(httpClient)
                .execute(
                    eq("PUT"),
                    eq(urlWithRoute),
                    eq(ElasticUser.NAME),
                    eq(bootstrapPassword),
                    passwordCaptor.capture(),
                    anyCheckedFunction()
                );

            String userPassword = passwordCaptor.getValue().get();
            capturedPasswords.put(user, userPassword);
        }

        for (Map.Entry<String, String> entry : SetupPasswordTool.USERS_WITH_SHARED_PASSWORDS.entrySet()) {
            assertEquals(capturedPasswords.get(entry.getKey()), capturedPasswords.get(entry.getValue()));

            capturedPasswords.remove(entry.getKey());
            capturedPasswords.remove(entry.getValue());
        }

        Set<String> uniqueCapturedPasswords = new HashSet<>(capturedPasswords.values());
        assertEquals(uniqueCapturedPasswords.size(), capturedPasswords.size());
    }

    public void testAuthnFail() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_UNAUTHORIZED, new HashMap<>());

        when(
            httpClient.execute(
                eq("GET"),
                eq(authnURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponse);

        try {
            execute(randomBoolean() ? "auto" : "interactive");
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.CONFIG, e.exitCode);
        }
    }

    public void testErrorMessagesWhenXPackIsNotAvailableOnNode() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_NOT_FOUND, new HashMap<>());
        when(
            httpClient.execute(
                eq("GET"),
                eq(authnURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponse);

        URL xpackSecurityPluginQueryURL = queryXPackSecurityFeatureConfigURL(url);
        String securityPluginQueryResponseBody = null;
        final IllegalArgumentException illegalArgException = new IllegalArgumentException(
            "request [/_xpack] contains unrecognized parameter: [categories]"
        );
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject();
            ElasticsearchException.generateFailureXContent(builder, ToXContent.EMPTY_PARAMS, illegalArgException, true);
            builder.field("status", RestStatus.BAD_REQUEST.getStatus());
            builder.endObject();
            securityPluginQueryResponseBody = Strings.toString(builder);
        }
        when(
            httpClient.execute(
                eq("GET"),
                eq(xpackSecurityPluginQueryURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(createHttpResponse(HttpURLConnection.HTTP_BAD_REQUEST, securityPluginQueryResponseBody));

        thrown.expect(UserException.class);
        thrown.expectMessage("X-Pack is not available on this Elasticsearch node.");
        execute(randomBoolean() ? "auto" : "interactive");
    }

    public void testErrorMessagesWhenXPackIsAvailableWithCorrectLicenseAndIsEnabledButStillFailedForUnknown() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_NOT_FOUND, new HashMap<>());
        when(
            httpClient.execute(
                eq("GET"),
                eq(authnURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponse);

        URL xpackSecurityPluginQueryURL = queryXPackSecurityFeatureConfigURL(url);

        Set<FeatureSet> featureSets = new HashSet<>();
        featureSets.add(new FeatureSet("logstash", true, true));
        featureSets.add(new FeatureSet("security", true, true));
        FeatureSetsInfo featureInfos = new FeatureSetsInfo(featureSets);
        XPackInfoResponse xpackInfo = new XPackInfoResponse(null, null, featureInfos);
        String securityPluginQueryResponseBody = null;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject();
            builder.field("features", xpackInfo.getFeatureSetsInfo());
            builder.endObject();
            securityPluginQueryResponseBody = Strings.toString(builder);
        }
        when(
            httpClient.execute(
                eq("GET"),
                eq(xpackSecurityPluginQueryURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, securityPluginQueryResponseBody));

        thrown.expect(UserException.class);
        thrown.expectMessage("Unknown error");
        execute(randomBoolean() ? "auto" : "interactive");

    }

    public void testErrorMessagesWhenXPackPluginIsAvailableButNoSecurityLicense() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);
        URL xpackSecurityPluginQueryURL = queryXPackSecurityFeatureConfigURL(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_NOT_FOUND, new HashMap<>());
        when(
            httpClient.execute(
                eq("GET"),
                eq(authnURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponse);

        Set<FeatureSet> featureSets = new HashSet<>();
        featureSets.add(new FeatureSet("logstash", true, true));
        featureSets.add(new FeatureSet("security", false, false));
        FeatureSetsInfo featureInfos = new FeatureSetsInfo(featureSets);
        XPackInfoResponse xpackInfo = new XPackInfoResponse(null, null, featureInfos);
        String securityPluginQueryResponseBody = null;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject();
            builder.field("features", xpackInfo.getFeatureSetsInfo());
            builder.endObject();
            securityPluginQueryResponseBody = Strings.toString(builder);
        }
        when(
            httpClient.execute(
                eq("GET"),
                eq(xpackSecurityPluginQueryURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, securityPluginQueryResponseBody));

        thrown.expect(UserException.class);
        thrown.expectMessage("X-Pack Security is not available.");
        execute(randomBoolean() ? "auto" : "interactive");

    }

    public void testErrorMessagesWhenXPackPluginIsAvailableWithValidLicenseButDisabledSecurity() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);
        URL xpackSecurityPluginQueryURL = queryXPackSecurityFeatureConfigURL(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_NOT_FOUND, new HashMap<>());
        when(
            httpClient.execute(
                eq("GET"),
                eq(authnURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponse);

        Set<FeatureSet> featureSets = new HashSet<>();
        featureSets.add(new FeatureSet("logstash", true, true));
        featureSets.add(new FeatureSet("security", true, false));
        FeatureSetsInfo featureInfos = new FeatureSetsInfo(featureSets);
        XPackInfoResponse xpackInfo = new XPackInfoResponse(null, null, featureInfos);
        String securityPluginQueryResponseBody = null;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject();
            builder.field("features", xpackInfo.getFeatureSetsInfo());
            builder.endObject();
            securityPluginQueryResponseBody = Strings.toString(builder);
        }
        when(
            httpClient.execute(
                eq("GET"),
                eq(xpackSecurityPluginQueryURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, securityPluginQueryResponseBody));

        thrown.expect(UserException.class);
        thrown.expectMessage("X-Pack Security is disabled by configuration.");
        execute(randomBoolean() ? "auto" : "interactive");
    }

    public void testWrongServer() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);
        doThrow(randomFrom(new IOException(), new SSLException(""))).when(httpClient)
            .execute(eq("GET"), eq(authnURL), eq(ElasticUser.NAME), any(SecureString.class), anyCheckedSupplier(), anyCheckedFunction());

        try {
            execute(randomBoolean() ? "auto" : "interactive");
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.CONFIG, e.exitCode);
        }
    }

    public void testRedCluster() throws Exception {
        resetSecretsAndKeyStore("n");
        URL url = new URL(httpClient.getDefaultURL());

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<>());
        when(
            httpClient.execute(
                eq("GET"),
                eq(authenticateUrl(url)),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponse);

        httpResponse = new HttpResponse(
            HttpURLConnection.HTTP_OK,
            Map.of("cluster_name", "elasticsearch", "status", "red", "number_of_nodes", 1)
        );
        when(
            httpClient.execute(
                eq("GET"),
                eq(clusterHealthUrl(url)),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponse);

        try {
            execute(randomBoolean() ? "auto" : "interactive");
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.OK, e.exitCode);
            assertThat(terminal.getErrorOutput(), Matchers.containsString("Your cluster health is currently RED."));
        }
    }

    public void testUrlOption() throws Exception {
        URL url = new URL("http://localhost:9202" + randomFrom("", "/", "//", "/smth", "//smth/", "//x//x/"));
        execute("auto", "-u", url.toString(), "-b");

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = authenticateUrl(url);
        inOrder.verify(httpClient)
            .execute(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), anyCheckedSupplier(), anyCheckedFunction());
        for (String user : usersInSetOrder) {
            URL urlWithRoute = passwordUrl(url, user);
            inOrder.verify(httpClient)
                .execute(
                    eq("PUT"),
                    eq(urlWithRoute),
                    eq(ElasticUser.NAME),
                    eq(bootstrapPassword),
                    anyCheckedSupplier(),
                    anyCheckedFunction()
                );
        }
    }

    public void testSetUserPassFail() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        String userToFail = randomFrom(SetupPasswordTool.USERS);
        URL userToFailURL = passwordUrl(url, userToFail);

        doThrow(new IOException()).when(httpClient)
            .execute(eq("PUT"), eq(userToFailURL), anyString(), any(SecureString.class), anyCheckedSupplier(), anyCheckedFunction());
        try {
            execute(randomBoolean() ? "auto" : "interactive", "-b");
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.TEMP_FAILURE, e.exitCode);
        }
    }

    public void testInteractiveSetup() throws Exception {
        resetSecretsAndKeyStore("Y");

        URL url = new URL(httpClient.getDefaultURL());
        execute("interactive");

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = authenticateUrl(url);
        inOrder.verify(httpClient)
            .execute(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), anyCheckedSupplier(), anyCheckedFunction());

        for (String user : usersInSetOrder) {
            URL urlWithRoute = passwordUrl(url, user);
            @SuppressWarnings("unchecked")
            ArgumentCaptor<CheckedSupplier<String, Exception>> passwordCaptor = ArgumentCaptor.forClass((Class) CheckedSupplier.class);
            inOrder.verify(httpClient)
                .execute(
                    eq("PUT"),
                    eq(urlWithRoute),
                    eq(ElasticUser.NAME),
                    eq(bootstrapPassword),
                    passwordCaptor.capture(),
                    anyCheckedFunction()
                );
            assertThat(passwordCaptor.getValue().get(), containsString(getExpectedPasswordForUser(user)));
        }
    }

    public void testInteractivePasswordsFatFingers() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());

        terminal.reset();
        if (usedKeyStore.hasPassword()) {
            terminal.addSecretInput("keystore-password");
        }
        terminal.addTextInput("Y");
        for (String user : SetupPasswordTool.USERS) {
            if (SetupPasswordTool.USERS_WITH_SHARED_PASSWORDS.containsValue(user)) {
                continue;
            }

            // fail in strength and match
            int failCount = randomIntBetween(3, 10);
            while (failCount-- > 0) {
                String password1 = randomAlphaOfLength(randomIntBetween(3, 10));
                terminal.addSecretInput(password1);
                Validation.Error err = Validation.Users.validatePassword(new SecureString(password1.toCharArray()));
                if (err == null) {
                    // passes strength validation, fail by mismatch
                    terminal.addSecretInput(password1 + "typo");
                }
            }
            // two good passwords
            terminal.addSecretInput(user + "-password");
            terminal.addSecretInput(user + "-password");
        }

        execute("interactive");

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = authenticateUrl(url);
        inOrder.verify(httpClient)
            .execute(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), anyCheckedSupplier(), anyCheckedFunction());
        for (String user : usersInSetOrder) {
            URL urlWithRoute = passwordUrl(url, user);
            @SuppressWarnings({ "unchecked", "rawtypes" })
            ArgumentCaptor<CheckedSupplier<String, Exception>> passwordCaptor = ArgumentCaptor.forClass((Class) CheckedSupplier.class);
            inOrder.verify(httpClient)
                .execute(
                    eq("PUT"),
                    eq(urlWithRoute),
                    eq(ElasticUser.NAME),
                    eq(bootstrapPassword),
                    passwordCaptor.capture(),
                    anyCheckedFunction()
                );
            assertThat(passwordCaptor.getValue().get(), containsString(getExpectedPasswordForUser(user)));
        }
    }

    public void testWrongKeystorePassword() throws Exception {
        Command commandWithPasswordProtectedKeystore = getSetupPasswordCommandWithKeyStore(passwordProtectedKeystore);
        terminal.reset();
        terminal.addSecretInput("wrong-password");
        final UserException e = expectThrows(UserException.class, () -> {
            if (randomBoolean()) {
                execute(commandWithPasswordProtectedKeystore, "auto", "-b", "true");
            } else {
                terminal.addTextInput("Y");
                execute(commandWithPasswordProtectedKeystore, "auto");
            }
        });
        assertThat(e.getMessage(), containsString("Provided keystore password was incorrect"));
    }

    private URL authenticateUrl(URL url) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_security/_authenticate").replaceAll("/+", "/") + "?pretty");
    }

    private URL passwordUrl(URL url, String user) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_security/user/" + user + "/_password").replaceAll("/+", "/") + "?pretty");
    }

    private URL clusterHealthUrl(URL url) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_cluster/health").replaceAll("/+", "/") + "?pretty");
    }

    private URL queryXPackSecurityFeatureConfigURL(URL url) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_xpack").replaceAll("/+", "/") + "?categories=features&human=false&pretty");
    }

    private HttpResponse createHttpResponse(final int httpStatus, final String responseJson) throws IOException {
        HttpResponseBuilder builder = new HttpResponseBuilder();
        builder.withHttpStatus(httpStatus);
        builder.withResponseBody(responseJson);
        return builder.build();
    }

    private Command getSetupPasswordCommandWithKeyStore(KeyStoreWrapper keyStore) {
        return new SetupPasswordTool(env -> httpClient, (e) -> keyStore) {

            @Override
            protected AutoSetup newAutoSetup() {
                return new AutoSetup();
            }

            @Override
            protected InteractiveSetup newInteractiveSetup() {
                return new InteractiveSetup();
            }

        };

    }

    private String getExpectedPasswordForUser(String user) throws Exception {
        if (SetupPasswordTool.USERS_WITH_SHARED_PASSWORDS.containsValue(user)) {
            for (Map.Entry<String, String> entry : SetupPasswordTool.USERS_WITH_SHARED_PASSWORDS.entrySet()) {
                if (entry.getValue().equals(user)) {
                    return entry.getKey() + "-password";
                }
            }
            throw new Exception("Expected to find corresponding user for " + user);
        }
        return user + "-password";
    }
}
