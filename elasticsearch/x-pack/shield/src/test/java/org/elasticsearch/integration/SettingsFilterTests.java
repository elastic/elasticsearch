/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.ssl.AbstractSSLService;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.After;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = SUITE)
public class SettingsFilterTests extends ShieldIntegTestCase {
    private CloseableHttpClient httpClient = HttpClients.createDefault();

    @After
    public void cleanup() throws IOException {
        httpClient.close();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> classes = super.nodePlugins();
        ArrayList<Class<? extends Plugin>> newClasses = new ArrayList<>(classes);
        newClasses.add(TestPlugin.class);
        return newClasses;
    }

    public static class TestPlugin extends Plugin {

        public TestPlugin() {}

        @Override
        public String name() {
            return "test_settings_adder";
        }

        @Override
        public String description() {
            return "adds some settings this test uses";
        }

        public void onModule(SettingsModule module) {
            module.registerSetting(Setting.simpleString("foo.bar", Setting.Property.NodeScope));
            module.registerSetting(Setting.simpleString("foo.baz", Setting.Property.NodeScope));
            module.registerSetting(Setting.simpleString("bar.baz", Setting.Property.NodeScope));
            module.registerSetting(Setting.simpleString("baz.foo", Setting.Property.NodeScope));
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        int clientProfilePort = randomIntBetween(49000, 65400);
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)

                .put("shield.authc.realms.file.type", "file")

                // ldap realm filtering
                .put("shield.authc.realms.ldap1.type", "ldap")
                .put("shield.authc.realms.ldap1.enabled", "false")
                .put("shield.authc.realms.ldap1.url", "ldap://host.domain")
                .put("shield.authc.realms.ldap1.hostname_verification", randomAsciiOfLength(5))
                .put("shield.authc.realms.ldap1.bind_dn", randomAsciiOfLength(5))
                .put("shield.authc.realms.ldap1.bind_password", randomAsciiOfLength(5))

                // active directory filtering
                .put("shield.authc.realms.ad1.type", "active_directory")
                .put("shield.authc.realms.ad1.enabled", "false")
                .put("shield.authc.realms.ad1.url", "ldap://host.domain")
                .put("shield.authc.realms.ad1.hostname_verification", randomAsciiOfLength(5))

                // pki filtering
                .put("shield.authc.realms.pki1.type", "pki")
                .put("shield.authc.realms.pki1.order", "0")
                .put("shield.authc.realms.pki1.truststore.path",
                        getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                .put("shield.authc.realms.pki1.truststore.password", "truststore-testnode-only")
                .put("shield.authc.realms.pki1.truststore.algorithm", "SunX509")

                .put("shield.ssl.keystore.path", "/path/to/keystore")
                .put("shield.ssl.ciphers", "_ciphers")
                .put("shield.ssl.supported_protocols", randomFrom(AbstractSSLService.DEFAULT_SUPPORTED_PROTOCOLS))
                .put("shield.ssl.keystore.password", randomAsciiOfLength(5))
                .put("shield.ssl.keystore.algorithm", "_algorithm")
                .put("shield.ssl.keystore.key_password", randomAsciiOfLength(5))
                .put("shield.ssl.truststore.password", randomAsciiOfLength(5))
                .put("shield.ssl.truststore.algorithm", "_algorithm")

                // client profile
                .put("transport.profiles.client.port", clientProfilePort + "-" + (clientProfilePort + 100))
                .put("transport.profiles.client.shield.keystore.path", "/path/to/keystore")
                .put("transport.profiles.client.shield.ciphers", "_ciphers")
                .put("transport.profiles.client.shield.supported_protocols", randomFrom(AbstractSSLService.DEFAULT_SUPPORTED_PROTOCOLS))
                .put("transport.profiles.client.shield.keystore.password", randomAsciiOfLength(5))
                .put("transport.profiles.client.shield.keystore.algorithm", "_algorithm")
                .put("transport.profiles.client.shield.keystore.key_password", randomAsciiOfLength(5))
                .put("transport.profiles.client.shield.truststore.password", randomAsciiOfLength(5))
                .put("transport.profiles.client.shield.truststore.algorithm", "_algorithm")

                // custom settings
                .put("foo.bar", "_secret")
                .put("foo.baz", "_secret")
                .put("bar.baz", "_secret")
                .put("baz.foo", "_not_a_secret") // should not be filtered
                .put("shield.hide_settings", "foo.*,bar.baz")
                .build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return false;
    }

    public void testFiltering() throws Exception {
        HttpResponse response = executeRequest("GET", "/_nodes", null, Collections.<String, String>emptyMap());
        List<Settings> list = extractSettings(response.getBody());
        for (Settings settings : list) {

            assertThat(settings.get("shield.authc.realms.ldap1.hostname_verification"), nullValue());
            assertThat(settings.get("shield.authc.realms.ldap1.bind_password"), nullValue());
            assertThat(settings.get("shield.authc.realms.ldap1.bind_dn"), nullValue());
            assertThat(settings.get("shield.authc.realms.ldap1.url"), is("ldap://host.domain"));

            assertThat(settings.get("shield.authc.realms.ad1.hostname_verification"), nullValue());
            assertThat(settings.get("shield.authc.realms.ad1.url"), is("ldap://host.domain"));

            assertThat(settings.get("shield.authc.realms.pki1.truststore.path"), nullValue());
            assertThat(settings.get("shield.authc.realms.pki1.truststore.password"), nullValue());
            assertThat(settings.get("shield.authc.realms.pki1.truststore.algorithm"), nullValue());
            assertThat(settings.get("shield.authc.realms.pki1.type"), is("pki"));

            assertThat(settings.get("shield.ssl.keystore.path"), nullValue());
            assertThat(settings.get("shield.ssl.ciphers"), nullValue());
            assertThat(settings.get("shield.ssl.supported_protocols"), nullValue());
            assertThat(settings.get("shield.ssl.keystore.password"), nullValue());
            assertThat(settings.get("shield.ssl.keystore.algorithm"), nullValue());
            assertThat(settings.get("shield.ssl.keystore.key_password"), nullValue());
            assertThat(settings.get("shield.ssl.truststore.password"), nullValue());
            assertThat(settings.get("shield.ssl.truststore.algorithm"), nullValue());

            // the client profile settings is also filtered out
            assertThat(settings.get("transport.profiles.client.port"), notNullValue());
            assertThat(settings.get("transport.profiles.client.shield.keystore.path"), nullValue());
            assertThat(settings.get("transport.profiles.client.shield.ciphers"), nullValue());
            assertThat(settings.get("transport.profiles.client.shield.supported_protocols"), nullValue());
            assertThat(settings.get("transport.profiles.client.shield.keystore.password"), nullValue());
            assertThat(settings.get("transport.profiles.client.shield.keystore.algorithm"), nullValue());
            assertThat(settings.get("transport.profiles.client.shield.keystore.key_password"), nullValue());
            assertThat(settings.get("transport.profiles.client.shield.truststore.password"), nullValue());
            assertThat(settings.get("transport.profiles.client.shield.truststore.algorithm"), nullValue());

            assertThat(settings.get("shield.hide_settings"), nullValue());
            assertThat(settings.get("foo.bar"), nullValue());
            assertThat(settings.get("foo.baz"), nullValue());
            assertThat(settings.get("bar.baz"), nullValue());
            assertThat(settings.get("baz.foo"), is("_not_a_secret"));
        }
    }

    static List<Settings> extractSettings(String data) throws Exception {
        List<Settings> settingsList = new ArrayList<>();
        XContentParser parser = JsonXContent.jsonXContent.createParser(data.getBytes(StandardCharsets.UTF_8));
        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != null) {
            if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals("settings")) {
                parser.nextToken();
                XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent());
                settingsList.add(Settings.builder().loadFromSource(builder.copyCurrentStructure(parser).bytes().toUtf8()).build());
            }
        }
        return settingsList;
    }

    protected HttpResponse executeRequest(String method, String uri, String body, Map<String, String> params) throws IOException {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);
        HttpRequestBuilder requestBuilder = new HttpRequestBuilder(httpClient)
                .httpTransport(httpServerTransport)
                .method(method)
                .path(uri);

        for (Map.Entry<String, String> entry : params.entrySet()) {
            requestBuilder.addParam(entry.getKey(), entry.getValue());
        }
        if (body != null) {
            requestBuilder.body(body);
        }
        requestBuilder.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                UsernamePasswordToken.basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                        new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray())));
        return requestBuilder.execute();
    }
}
