/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.support.Validation;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetupPasswordToolTests extends CommandTestCase {

    private final String pathHomeParameter = "-Epath.home=" + createTempDir();
    private SecureString bootstrapPassword;
    private CommandLineHttpClient httpClient;
    private KeyStoreWrapper keyStore;
    private List<String> usersInSetOrder;

    @Before
    public void setSecretsAndKeyStore() throws Exception {
        // sometimes we fall back to the keystore seed as this is the default when a new node starts
        boolean useFallback = randomBoolean();
        bootstrapPassword = useFallback ?  new SecureString("0xCAFEBABE".toCharArray()) :
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

        when(httpClient.postURL(anyString(), any(URL.class), anyString(), any(SecureString.class), any(CheckedSupplier.class),
                any(CheckedConsumer.class))).thenReturn(HttpURLConnection.HTTP_OK);

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
        return new SetupPasswordTool((e) -> httpClient, (e) -> keyStore) {

            @Override
            protected AutoSetup newAutoSetup() {
                return new AutoSetup() {
                    @Override
                    protected Environment createEnv(Terminal terminal, Map<String, String> settings) throws UserException {
                        Settings.Builder builder = Settings.builder();
                        settings.forEach((k,v) -> builder.put(k, v));
                        return TestEnvironment.newEnvironment(builder.build());
                    }
                };
            }

            @Override
            protected InteractiveSetup newInteractiveSetup() {
                return new InteractiveSetup() {
                    @Override
                    protected Environment createEnv(Terminal terminal, Map<String, String> settings) throws UserException {
                        Settings.Builder builder = Settings.builder();
                        settings.forEach((k,v) -> builder.put(k, v));
                        return TestEnvironment.newEnvironment(builder.build());
                    }
                };
            }

        };
    }

    public void testAutoSetup() throws Exception {
        String url = httpClient.getDefaultURL();
        execute("auto", pathHomeParameter, "-b", "true");

        verify(keyStore).decrypt(new char[0]);

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = new URL(url + "/_xpack/security/_authenticate?pretty");
        inOrder.verify(httpClient).postURL(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), any(CheckedSupplier.class),
                any(CheckedConsumer.class));
        for (String user : usersInSetOrder) {
            URL urlWithRoute = new URL(url + "/_xpack/security/user/" + user + "/_password");
            inOrder.verify(httpClient).postURL(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(bootstrapPassword),
                    any(CheckedSupplier.class), any(CheckedConsumer.class));
        }

    }

    public void testAuthnFail() throws Exception {
        URL authnURL = new URL(httpClient.getDefaultURL() + "/_xpack/security/_authenticate?pretty");
        when(httpClient.postURL(eq("GET"), eq(authnURL), eq(ElasticUser.NAME), any(SecureString.class), any(CheckedSupplier.class),
                any(CheckedConsumer.class))).thenReturn(HttpURLConnection.HTTP_UNAUTHORIZED);

        try {
            execute(randomBoolean() ? "auto" : "interactive", pathHomeParameter);
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.CONFIG, e.exitCode);
        }

    }

    public void testUrlOption() throws Exception {
        String url = "http://localhost:9202";
        execute("auto", pathHomeParameter, "-u", url, "-b");

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = new URL(url + "/_xpack/security/_authenticate?pretty");
        inOrder.verify(httpClient).postURL(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), any(CheckedSupplier.class),
                any(CheckedConsumer.class));
        for (String user : usersInSetOrder) {
            URL urlWithRoute = new URL(url + "/_xpack/security/user/" + user + "/_password");
            inOrder.verify(httpClient).postURL(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(bootstrapPassword),
                    any(CheckedSupplier.class), any(CheckedConsumer.class));
        }
    }

    public void testInteractiveSetup() throws Exception {
        String url = httpClient.getDefaultURL();

        terminal.addTextInput("Y");
        execute("interactive", pathHomeParameter);

        InOrder inOrder = Mockito.inOrder(httpClient);

        URL checkUrl = new URL(url + "/_xpack/security/_authenticate?pretty");
        inOrder.verify(httpClient).postURL(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), any(CheckedSupplier.class),
                any(CheckedConsumer.class));
        for (String user : usersInSetOrder) {
            URL urlWithRoute = new URL(url + "/_xpack/security/user/" + user + "/_password");
            ArgumentCaptor<CheckedSupplier<String, Exception>> passwordCaptor = ArgumentCaptor.forClass((Class) CheckedSupplier.class);
            inOrder.verify(httpClient).postURL(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(bootstrapPassword),
                    passwordCaptor.capture(), any(CheckedConsumer.class));
            assertThat(passwordCaptor.getValue().get(), CoreMatchers.containsString(user + "-password"));
        }
    }

    public void testInteractivePasswordsFatFingers() throws Exception {
        String url = httpClient.getDefaultURL();

        terminal.reset();
        terminal.addTextInput("Y");
        for (String user : SetupPasswordTool.USERS) {
            // fail in strength and match
            int failCount = randomIntBetween(3, 10);
            while (failCount-- > 0) {
                String password1 = randomAlphaOfLength(randomIntBetween(3, 10));
                terminal.addSecretInput(password1);
                Validation.Error err = Validation.Users.validatePassword(password1.toCharArray());
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

        URL checkUrl = new URL(url + "/_xpack/security/_authenticate?pretty");
        inOrder.verify(httpClient).postURL(eq("GET"), eq(checkUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), any(CheckedSupplier.class),
                any(CheckedConsumer.class));
        for (String user : usersInSetOrder) {
            URL urlWithRoute = new URL(url + "/_xpack/security/user/" + user + "/_password");
            ArgumentCaptor<CheckedSupplier<String, Exception>> passwordCaptor = ArgumentCaptor.forClass((Class) CheckedSupplier.class);
            inOrder.verify(httpClient).postURL(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(bootstrapPassword),
                    passwordCaptor.capture(), any(CheckedConsumer.class));
            assertThat(passwordCaptor.getValue().get(), CoreMatchers.containsString(user + "-password"));
        }
    }

    private String parsePassword(String value) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, value)) {
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
}
