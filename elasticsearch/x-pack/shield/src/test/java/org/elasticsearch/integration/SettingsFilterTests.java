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
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
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
            module.registerSetting(Setting.simpleString("foo.bar", Property.NodeScope));
            module.registerSetting(Setting.simpleString("foo.baz", Property.NodeScope));
            module.registerSetting(Setting.simpleString("bar.baz", Property.NodeScope));
            module.registerSetting(Setting.simpleString("baz.foo", Property.NodeScope));
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        int clientProfilePort = randomIntBetween(49000, 65400);
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)

                .put("xpack.security.authc.realms.file.type", "file")

                // ldap realm filtering
                .put("xpack.security.authc.realms.ldap1.type", "ldap")
                .put("xpack.security.authc.realms.ldap1.enabled", "false")
                .put("xpack.security.authc.realms.ldap1.url", "ldap://host.domain")
                .put("xpack.security.authc.realms.ldap1.hostname_verification", randomAsciiOfLength(5))
                .put("xpack.security.authc.realms.ldap1.bind_dn", randomAsciiOfLength(5))
                .put("xpack.security.authc.realms.ldap1.bind_password", randomAsciiOfLength(5))

                // active directory filtering
                .put("xpack.security.authc.realms.ad1.type", "active_directory")
                .put("xpack.security.authc.realms.ad1.enabled", "false")
                .put("xpack.security.authc.realms.ad1.url", "ldap://host.domain")
                .put("xpack.security.authc.realms.ad1.hostname_verification", randomAsciiOfLength(5))

                // pki filtering
                .put("xpack.security.authc.realms.pki1.type", "pki")
                .put("xpack.security.authc.realms.pki1.order", "0")
                .put("xpack.security.authc.realms.pki1.truststore.path",
                        getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                .put("xpack.security.authc.realms.pki1.truststore.password", "truststore-testnode-only")
                .put("xpack.security.authc.realms.pki1.truststore.algorithm", "SunX509")

                .put("xpack.security.ssl.keystore.path", "/path/to/keystore")
                .put("xpack.security.ssl.ciphers", "_ciphers")
                .put("xpack.security.ssl.supported_protocols", randomFrom(Global.DEFAULT_SUPPORTED_PROTOCOLS))
                .put("xpack.security.ssl.keystore.password", randomAsciiOfLength(5))
                .put("xpack.security.ssl.keystore.algorithm", "_algorithm")
                .put("xpack.security.ssl.keystore.key_password", randomAsciiOfLength(5))
                .put("xpack.security.ssl.truststore.password", randomAsciiOfLength(5))
                .put("xpack.security.ssl.truststore.algorithm", "_algorithm")

                // client profile
                .put("transport.profiles.client.port", clientProfilePort + "-" + (clientProfilePort + 100))
                .put("transport.profiles.client.xpack.security.keystore.path", "/path/to/keystore")
                .put("transport.profiles.client.xpack.security.ciphers", "_ciphers")
                .put("transport.profiles.client.xpack.security.supported_protocols",
                        randomFrom(Global.DEFAULT_SUPPORTED_PROTOCOLS))
                .put("transport.profiles.client.xpack.security.keystore.password", randomAsciiOfLength(5))
                .put("transport.profiles.client.xpack.security.keystore.algorithm", "_algorithm")
                .put("transport.profiles.client.xpack.security.keystore.key_password", randomAsciiOfLength(5))
                .put("transport.profiles.client.xpack.security.truststore.password", randomAsciiOfLength(5))
                .put("transport.profiles.client.xpack.security.truststore.algorithm", "_algorithm")

                // custom settings
                .put("foo.bar", "_secret")
                .put("foo.baz", "_secret")
                .put("bar.baz", "_secret")
                .put("baz.foo", "_not_a_secret") // should not be filtered
                .put("xpack.security.hide_settings", "foo.*,bar.baz")
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

            assertThat(settings.get("xpack.security.authc.realms.ldap1.hostname_verification"), nullValue());
            assertThat(settings.get("xpack.security.authc.realms.ldap1.bind_password"), nullValue());
            assertThat(settings.get("xpack.security.authc.realms.ldap1.bind_dn"), nullValue());
            assertThat(settings.get("xpack.security.authc.realms.ldap1.url"), is("ldap://host.domain"));

            assertThat(settings.get("xpack.security.authc.realms.ad1.hostname_verification"), nullValue());
            assertThat(settings.get("xpack.security.authc.realms.ad1.url"), is("ldap://host.domain"));

            assertThat(settings.get("xpack.security.authc.realms.pki1.truststore.path"), nullValue());
            assertThat(settings.get("xpack.security.authc.realms.pki1.truststore.password"), nullValue());
            assertThat(settings.get("xpack.security.authc.realms.pki1.truststore.algorithm"), nullValue());
            assertThat(settings.get("xpack.security.authc.realms.pki1.type"), is("pki"));

            assertThat(settings.get("xpack.security.ssl.keystore.path"), nullValue());
            assertThat(settings.get("xpack.security.ssl.ciphers"), nullValue());
            assertThat(settings.get("xpack.security.ssl.supported_protocols"), nullValue());
            assertThat(settings.get("xpack.security.ssl.keystore.password"), nullValue());
            assertThat(settings.get("xpack.security.ssl.keystore.algorithm"), nullValue());
            assertThat(settings.get("xpack.security.ssl.keystore.key_password"), nullValue());
            assertThat(settings.get("xpack.security.ssl.truststore.password"), nullValue());
            assertThat(settings.get("xpack.security.ssl.truststore.algorithm"), nullValue());

            // the client profile settings is also filtered out
            assertThat(settings.get("transport.profiles.client.port"), notNullValue());
            assertThat(settings.get("transport.profiles.client.xpack.security.keystore.path"), nullValue());
            assertThat(settings.get("transport.profiles.client.xpack.security.ciphers"), nullValue());
            assertThat(settings.get("transport.profiles.client.xpack.security.supported_protocols"), nullValue());
            assertThat(settings.get("transport.profiles.client.xpack.security.keystore.password"), nullValue());
            assertThat(settings.get("transport.profiles.client.xpack.security.keystore.algorithm"), nullValue());
            assertThat(settings.get("transport.profiles.client.xpack.security.keystore.key_password"), nullValue());
            assertThat(settings.get("transport.profiles.client.xpack.security.truststore.password"), nullValue());
            assertThat(settings.get("transport.profiles.client.xpack.security.truststore.algorithm"), nullValue());

            assertThat(settings.get("xpack.security.hide_settings"), nullValue());
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
