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
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SetupPasswordToolTests extends CommandTestCase {

    private final String pathHomeParameter = "-Epath.home=" + createTempDir();
    private SecureString bootstrapPassword;
    private final String ep = "elastic-password";
    private final String kp = "kibana-password";
    private final String lp = "logstash-password";
    private CommandLineHttpClient httpClient;
    private KeyStoreWrapper keyStore;

    @Before
    public void setSecretsAndKeyStore() throws GeneralSecurityException {
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

        terminal.addSecretInput(ep);
        terminal.addSecretInput(ep);
        terminal.addSecretInput(kp);
        terminal.addSecretInput(kp);
        terminal.addSecretInput(lp);
        terminal.addSecretInput(lp);
    }

    @Override
    protected Command newCommand() {
        return new SetupPasswordTool((e) -> httpClient, (e) -> keyStore) {

            @Override
            protected AutoSetup newAutoSetup() {
                return new AutoSetup() {
                    @Override
                    protected Environment createEnv(Terminal terminal, Map<String, String> settings) throws UserException {
                        return new Environment(Settings.builder().put(settings).build());
                    }
                };
            }

            @Override
            protected InteractiveSetup newInteractiveSetup() {
                return new InteractiveSetup() {
                    @Override
                    protected Environment createEnv(Terminal terminal, Map<String, String> settings) throws UserException {
                        return new Environment(Settings.builder().put(settings).build());
                    }
                };
            }

        };
    }

    public void testAutoSetup() throws Exception {
        execute("auto", pathHomeParameter, "-b", "true");

        verify(keyStore).decrypt(new char[0]);

        ArgumentCaptor<String> passwordCaptor = ArgumentCaptor.forClass(String.class);

        InOrder inOrder = Mockito.inOrder(httpClient);
        String elasticUrl = "http://localhost:9200/_xpack/security/user/elastic/_password";
        inOrder.verify(httpClient).postURL(eq("PUT"), eq(elasticUrl), eq(ElasticUser.NAME), eq(bootstrapPassword),
                passwordCaptor.capture());

        String[] users = {KibanaUser.NAME, LogstashSystemUser.NAME};
        SecureString newPassword = new SecureString(parsePassword(passwordCaptor.getValue()).toCharArray());
        for (String user : users) {
            String urlWithRoute = "http://localhost:9200/_xpack/security/user/" + user + "/_password";
            inOrder.verify(httpClient).postURL(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(newPassword), anyString());
        }
    }

    public void testUrlOption() throws Exception {
        String url = "http://localhost:9202";
        execute("auto", pathHomeParameter, "-u", url, "-b");

        ArgumentCaptor<String> passwordCaptor = ArgumentCaptor.forClass(String.class);

        InOrder inOrder = Mockito.inOrder(httpClient);
        String elasticUrl = url + "/_xpack/security/user/elastic/_password";
        inOrder.verify(httpClient).postURL(eq("PUT"), eq(elasticUrl), eq(ElasticUser.NAME), eq(bootstrapPassword),
                passwordCaptor.capture());

        String[] users = {KibanaUser.NAME, LogstashSystemUser.NAME};
        SecureString newPassword = new SecureString(parsePassword(passwordCaptor.getValue()).toCharArray());
        for (String user : users) {
            String urlWithRoute = url + "/_xpack/security/user/" + user + "/_password";
            inOrder.verify(httpClient).postURL(eq("PUT"), eq(urlWithRoute), eq(ElasticUser.NAME), eq(newPassword), anyString());
        }
    }

    public void testInteractiveSetup() throws Exception {
        terminal.addTextInput("Y");

        execute("interactive", pathHomeParameter);

        InOrder inOrder = Mockito.inOrder(httpClient);
        String elasticUrl = "http://localhost:9200/_xpack/security/user/elastic/_password";
        SecureString newPassword = new SecureString(ep.toCharArray());
        inOrder.verify(httpClient).postURL(eq("PUT"), eq(elasticUrl), eq(ElasticUser.NAME), eq(bootstrapPassword), contains(ep));

        String kibanaUrl = "http://localhost:9200/_xpack/security/user/" + KibanaUser.NAME + "/_password";
        inOrder.verify(httpClient).postURL(eq("PUT"), eq(kibanaUrl), eq(ElasticUser.NAME), eq(newPassword), contains(kp));
        String logstashUrl = "http://localhost:9200/_xpack/security/user/" + LogstashSystemUser.NAME + "/_password";
        inOrder.verify(httpClient).postURL(eq("PUT"), eq(logstashUrl), eq(ElasticUser.NAME), eq(newPassword), contains(lp));
    }

    public void testInteractivePasswordsNotMatching() throws Exception {
        String ep = "elastic-password";

        terminal.reset();
        terminal.addTextInput("Y");
        terminal.addSecretInput(ep);
        terminal.addSecretInput(ep + "typo");
        String url = "http://localhost:9200";

        try {
            execute("interactive", pathHomeParameter, "-u", url);
            fail("Should have thrown exception");
        } catch (UserException e) {
            assertEquals(ExitCodes.USAGE, e.exitCode);
            assertEquals("Passwords for user [elastic] do not match", e.getMessage());
        }

        verifyZeroInteractions(httpClient);
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
