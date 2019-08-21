/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.esnative.tool.HttpResponse.HttpResponseBuilder;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import javax.net.ssl.SSLException;
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetupPasswordToolTests extends CommandTestCase {

    private final String pathHomeParameter = "-Epath.home=" + createTempDir();
    private SecureString bootstrapPassword;
    private CommandLineHttpClient httpClient;
    private KeyStoreWrapper keyStore;
    private List<String> usersInSetOrder;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setSecretsAndKeyStore() throws Exception {
        // sometimes we fall back to the keystore seed as this is the default when a new node starts
        boolean useFallback = randomBoolean();
        bootstrapPassword = useFallback ? new SecureString("0xCAFEBABE".toCharArray()) :
                new SecureString("bootstrap-password".toCharArray());
        this.keyStore = mock(KeyStoreWrapper.class);
        this.httpClient = mock(CommandLineHttpClient.class);

        when(keyStore.isLoaded()).thenReturn(true);
        if (useFallback) {
            when(keyStore.getSettingNames()).thenReturn(new HashSet<>(Arrays.asList(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(),
                    KeyStoreWrapper.SEED_SETTING.getKey())));
            when(keyStore.getString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey())).thenReturn(bootstrapPassword);
        } else {
            when(keyStore.getSettingNames()).thenReturn(Collections.singleton(KeyStoreWrapper.SEED_SETTING.getKey()));
            when(keyStore.getString(KeyStoreWrapper.SEED_SETTING.getKey())).thenReturn(bootstrapPassword);
        }

        when(httpClient.getDefaultURL()).thenReturn("http://localhost:9200");

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<String, Object>());
        when(httpClient.execute(anyString(), any(URL.class), anyString(), any(SecureString.class), any(CheckedSupplier.class),
                any(CheckedFunction.class))).thenReturn(httpResponse);

        URL url = new URL(httpClient.getDefaultURL());
        httpResponse = new HttpResponse(HttpURLConnection.HTTP_OK, Collections.singletonMap("status", randomFrom("yellow", "green")));
        when(httpClient.execute(anyString(), eq(clusterHealthUrl(url)), anyString(), any(SecureString.class), any(CheckedSupplier.class),
                any(CheckedFunction.class))).thenReturn(httpResponse);

        URL xpackSecurityPluginQueryURL = queryXPackSecurityFeatureConfigURL(url);
        HttpResponse queryXPackSecurityConfigHttpResponse = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<String, Object>());
        when(httpClient.execute(eq("GET"), eq(xpackSecurityPluginQueryURL), anyString(), any(SecureString.class),
                any(CheckedSupplier.class), any(CheckedFunction.class))).thenReturn(queryXPackSecurityConfigHttpResponse);

        // elastic user is updated last
        usersInSetOrder = new ArrayList<>(SetupPasswordTool.USERS);
        for (int i = 0; i < usersInSetOrder.size() - 1; i++) {
            if (ElasticUser.NAME.equals(usersInSetOrder.get(i))) {
                Collections.swap(usersInSetOrder, i, i + 1);
            }
        }

        for (String user : SetupPasswordTool.USERS) {
            terminal.addSecretInput(user + "-password");
            terminal.addSecretInput(user + "-password");
        }
    }

    @Override
    protected Command newCommand() {
        return new SetupPasswordTool((e, s) -> httpClient, (e) -> keyStore) {

            @Override
            protected AutoSetup newAutoSetup() {
                return new AutoSetup() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        Settings.Builder builder = Settings.builder();
                        settings.forEach((k, v) -> builder.put(k, v));
                        return TestEnvironment.newEnvironment(builder.build());
                    }
                };
            }

            @Override
            protected InteractiveSetup newInteractiveSetup() {
                return new InteractiveSetup() {
                    @Override
                    protected Environment createEnv(Map<String, String> settings) throws UserException {
                        Settings.Builder builder = Settings.builder();
                        settings.forEach((k, v) -> builder.put(k, v));
                        return TestEnvironment.newEnvironment(builder.build());
                    }
                };
            }

        };
    }

    public void testAutoSetup() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        if (randomBoolean()) {
            execute("auto", pathHomeParameter, "-b", "true");
        } else {
            terminal.addTextInput("Y");
            execute("auto", pathHomeParameter);
        }

        verify(keyStore).decrypt(new char[0]);

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = authenticateUrl(url);
        inOrder.verify(httpClient).execute(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), any(CheckedSupplier.class),
                any(CheckedFunction.class));
        for (String user : usersInSetOrder) {
            URL urlWithRoute = passwordUrl(url, user);
            inOrder.verify(httpClient).execute(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(bootstrapPassword),
                    any(CheckedSupplier.class), any(CheckedFunction.class));
        }
    }

    public void testAuthnFail() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_UNAUTHORIZED, new HashMap<String, Object>());

        when(httpClient.execute(eq("GET"), eq(authnURL), eq(ElasticUser.NAME), any(SecureString.class), any(CheckedSupplier.class),
                any(CheckedFunction.class))).thenReturn(httpResponse);

        try {
            execute(randomBoolean() ? "auto" : "interactive", pathHomeParameter);
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.CONFIG, e.exitCode);
        }
    }

    public void testErrorMessagesWhenXPackIsNotAvailableOnNode() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_NOT_FOUND, new HashMap<String, Object>());
        when(httpClient.execute(eq("GET"), eq(authnURL), eq(ElasticUser.NAME), any(SecureString.class), any(CheckedSupplier.class),
                any(CheckedFunction.class))).thenReturn(httpResponse);

        URL xpackSecurityPluginQueryURL = queryXPackSecurityFeatureConfigURL(url);
        String securityPluginQueryResponseBody = null;
        final IllegalArgumentException illegalArgException =
                new IllegalArgumentException("request [/_xpack] contains unrecognized parameter: [categories]");
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject();
            ElasticsearchException.generateFailureXContent(builder, ToXContent.EMPTY_PARAMS, illegalArgException, true);
            builder.field("status", RestStatus.BAD_REQUEST.getStatus());
            builder.endObject();
            securityPluginQueryResponseBody = Strings.toString(builder);
        }
        when(httpClient.execute(eq("GET"), eq(xpackSecurityPluginQueryURL), eq(ElasticUser.NAME), any(SecureString.class),
                any(CheckedSupplier.class), any(CheckedFunction.class)))
                .thenReturn(createHttpResponse(HttpURLConnection.HTTP_BAD_REQUEST, securityPluginQueryResponseBody));

        thrown.expect(UserException.class);
        thrown.expectMessage("X-Pack is not available on this Elasticsearch node.");
        execute(randomBoolean() ? "auto" : "interactive", pathHomeParameter);
    }

    public void testErrorMessagesWhenXPackIsAvailableWithCorrectLicenseAndIsEnabledButStillFailedForUnknown() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_NOT_FOUND, new HashMap<String, Object>());
        when(httpClient.execute(eq("GET"), eq(authnURL), eq(ElasticUser.NAME), any(SecureString.class), any(CheckedSupplier.class),
                any(CheckedFunction.class))).thenReturn(httpResponse);

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
        when(httpClient.execute(eq("GET"), eq(xpackSecurityPluginQueryURL), eq(ElasticUser.NAME), any(SecureString.class),
                any(CheckedSupplier.class), any(CheckedFunction.class)))
                .thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, securityPluginQueryResponseBody));

        thrown.expect(UserException.class);
        thrown.expectMessage("Unknown error");
        execute(randomBoolean() ? "auto" : "interactive", pathHomeParameter);

    }

    public void testErrorMessagesWhenXPackPluginIsAvailableButNoSecurityLicense() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);
        URL xpackSecurityPluginQueryURL = queryXPackSecurityFeatureConfigURL(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_NOT_FOUND, new HashMap<String, Object>());
        when(httpClient.execute(eq("GET"), eq(authnURL), eq(ElasticUser.NAME), any(SecureString.class), any(CheckedSupplier.class),
                any(CheckedFunction.class))).thenReturn(httpResponse);

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
        when(httpClient.execute(eq("GET"), eq(xpackSecurityPluginQueryURL), eq(ElasticUser.NAME), any(SecureString.class),
                any(CheckedSupplier.class), any(CheckedFunction.class)))
                .thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, securityPluginQueryResponseBody));

        thrown.expect(UserException.class);
        thrown.expectMessage("X-Pack Security is not available.");
        execute(randomBoolean() ? "auto" : "interactive", pathHomeParameter);

    }

    public void testErrorMessagesWhenXPackPluginIsAvailableWithValidLicenseButDisabledSecurity() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);
        URL xpackSecurityPluginQueryURL = queryXPackSecurityFeatureConfigURL(url);

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_NOT_FOUND, new HashMap<String, Object>());
        when(httpClient.execute(eq("GET"), eq(authnURL), eq(ElasticUser.NAME), any(SecureString.class), any(CheckedSupplier.class),
                any(CheckedFunction.class))).thenReturn(httpResponse);

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
        when(httpClient.execute(eq("GET"), eq(xpackSecurityPluginQueryURL), eq(ElasticUser.NAME), any(SecureString.class),
                any(CheckedSupplier.class), any(CheckedFunction.class)))
                .thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, securityPluginQueryResponseBody));

        thrown.expect(UserException.class);
        thrown.expectMessage("X-Pack Security is disabled by configuration.");
        execute(randomBoolean() ? "auto" : "interactive", pathHomeParameter);
    }

    public void testWrongServer() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        URL authnURL = authenticateUrl(url);
        doThrow(randomFrom(new IOException(), new SSLException(""))).when(httpClient).execute(eq("GET"), eq(authnURL), eq(ElasticUser.NAME),
                any(SecureString.class), any(CheckedSupplier.class), any(CheckedFunction.class));

        try {
            execute(randomBoolean() ? "auto" : "interactive", pathHomeParameter);
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.CONFIG, e.exitCode);
        }
    }

    public void testRedCluster() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());

        HttpResponse httpResponse = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<>());
        when(httpClient.execute(eq("GET"), eq(authenticateUrl(url)), eq(ElasticUser.NAME), any(SecureString.class),
                any(CheckedSupplier.class), any(CheckedFunction.class))).thenReturn(httpResponse);

        httpResponse = new HttpResponse(HttpURLConnection.HTTP_OK, MapBuilder.<String, Object>newMapBuilder()
                .put("cluster_name", "elasticsearch").put("status", "red").put("number_of_nodes", 1).map());
        when(httpClient.execute(eq("GET"), eq(clusterHealthUrl(url)), eq(ElasticUser.NAME), any(SecureString.class),
                any(CheckedSupplier.class), any(CheckedFunction.class))).thenReturn(httpResponse);

        terminal.addTextInput("n");
        try {
            execute(randomBoolean() ? "auto" : "interactive", pathHomeParameter);
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.OK, e.exitCode);
            assertThat(terminal.getErrorOutput(), Matchers.containsString("Your cluster health is currently RED."));
        }
    }

    public void testUrlOption() throws Exception {
        URL url = new URL("http://localhost:9202" + randomFrom("", "/", "//", "/smth", "//smth/", "//x//x/"));
        execute("auto", pathHomeParameter, "-u", url.toString(), "-b");

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = authenticateUrl(url);
        inOrder.verify(httpClient).execute(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), any(CheckedSupplier.class),
                any(CheckedFunction.class));
        for (String user : usersInSetOrder) {
            URL urlWithRoute = passwordUrl(url, user);
            inOrder.verify(httpClient).execute(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(bootstrapPassword),
                    any(CheckedSupplier.class), any(CheckedFunction.class));
        }
    }

    public void testSetUserPassFail() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());
        String userToFail = randomFrom(SetupPasswordTool.USERS);
        URL userToFailURL = passwordUrl(url, userToFail);

        doThrow(new IOException()).when(httpClient).execute(eq("PUT"), eq(userToFailURL), anyString(), any(SecureString.class),
                any(CheckedSupplier.class), any(CheckedFunction.class));
        try {
            execute(randomBoolean() ? "auto" : "interactive", pathHomeParameter, "-b");
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.TEMP_FAILURE, e.exitCode);
        }
    }

    public void testInteractiveSetup() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());

        terminal.addTextInput("Y");
        execute("interactive", pathHomeParameter);

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = authenticateUrl(url);
        inOrder.verify(httpClient).execute(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), any(CheckedSupplier.class),
                any(CheckedFunction.class));
        for (String user : usersInSetOrder) {
            URL urlWithRoute = passwordUrl(url, user);
            ArgumentCaptor<CheckedSupplier<String, Exception>> passwordCaptor = ArgumentCaptor.forClass((Class) CheckedSupplier.class);
            inOrder.verify(httpClient).execute(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(bootstrapPassword),
                    passwordCaptor.capture(), any(CheckedFunction.class));
            assertThat(passwordCaptor.getValue().get(), CoreMatchers.containsString(user + "-password"));
        }
    }

    public void testInteractivePasswordsFatFingers() throws Exception {
        URL url = new URL(httpClient.getDefaultURL());

        terminal.reset();
        terminal.addTextInput("Y");
        for (String user : SetupPasswordTool.USERS) {
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

        execute("interactive", pathHomeParameter);

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = authenticateUrl(url);
        inOrder.verify(httpClient).execute(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), any(CheckedSupplier.class),
                any(CheckedFunction.class));
        for (String user : usersInSetOrder) {
            URL urlWithRoute = passwordUrl(url, user);
            ArgumentCaptor<CheckedSupplier<String, Exception>> passwordCaptor = ArgumentCaptor.forClass((Class) CheckedSupplier.class);
            inOrder.verify(httpClient).execute(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(bootstrapPassword),
                    passwordCaptor.capture(), any(CheckedFunction.class));
            assertThat(passwordCaptor.getValue().get(), CoreMatchers.containsString(user + "-password"));
        }
    }

    private String parsePassword(String value) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, value)) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
                        return parser.text();
                    }
                }
            }
        }
        throw new RuntimeException("Did not properly parse password.");
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
        return new URL(url,
                (url.toURI().getPath() + "/_xpack").replaceAll("/+", "/") + "?categories=features&human=false&pretty");
    }

    private HttpResponse createHttpResponse(final int httpStatus, final String responseJson) throws IOException {
        HttpResponseBuilder builder = new HttpResponseBuilder();
        builder.withHttpStatus(httpStatus);
        builder.withResponseBody(responseJson);
        return builder.build();
    }
}
