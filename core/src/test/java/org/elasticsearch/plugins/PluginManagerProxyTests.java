/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import io.netty.handler.codec.http.HttpRequest;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.PluginManager.OutputMode;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.After;
import org.junit.Before;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.ProxyAuthenticator;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.plugins.PluginManager.DEFAULT_TIMEOUT;
import static org.elasticsearch.plugins.PluginManager.setProxyPropertiesFromURL;
import static org.hamcrest.Matchers.*;

@LuceneTestCase.SuppressFileSystems("*") // due to jimfs, see PluginManagerTests
public class PluginManagerProxyTests extends ElasticsearchTestCase {

    public static final String PROXY_AUTH_USERNAME = "mySecretUser";
    public static final String PROXY_AUTH_PASSWORD = "mySecretPassword";

    private final int randomProxyPort = randomIntBetween(49152, 65535);
    private final List<HttpRequest> requests = new ArrayList<>();
    private Environment environment;

    private final ProxyAuthenticator proxyAuthenticator = new ProxyAuthenticator() {
        @Override
        public boolean authenticate(String userName, String password) {
            return PROXY_AUTH_USERNAME.equals(userName) && PROXY_AUTH_PASSWORD.equals(password);
        }
    };

    private final HttpFiltersSourceAdapter filtersSource = new HttpFiltersSourceAdapter() {
        @Override
        public HttpFilters filterRequest(HttpRequest originalRequest) {
            requests.add(originalRequest);
            return super.filterRequest(originalRequest);
        }
    };

    @Before
    public void setupEnvironment() throws Exception {
        Path homeDir = createTempDir();
        Files.createDirectory(homeDir.resolve("config"));
        Files.createDirectory(homeDir.resolve("plugins"));
        System.setProperty("es.path.home", homeDir.toString());
        environment = InternalSettingsPreparer.prepareSettings(EMPTY_SETTINGS, true, Terminal.DEFAULT).v2();
    }

    @After
    public void clearProperties() throws Exception {
        System.clearProperty("es.path.home");

        for (String protocol : new String[]{"http.", "https.", ""}) {
            System.clearProperty(protocol + "proxyHost");
            System.clearProperty(protocol + "proxyPort");
            System.clearProperty(protocol + "proxyUser");
            System.clearProperty(protocol + "proxyPassword");
        }
    }

    public void testThatProxyCanBeConfiguredViaProperty() throws Exception {
        assumeTrue("test requires security manager to be disabled", System.getSecurityManager() == null);

        System.setProperty("proxyHost", "localhost");
        System.setProperty("proxyPort", "" + randomProxyPort);

        HttpProxyServer server = DefaultHttpProxyServer.bootstrap()
                .withPort(randomProxyPort)
                .withFiltersSource(filtersSource)
                .start();

        try {
            PluginManager pluginManager = new PluginManager(environment, null, OutputMode.DEFAULT, DEFAULT_TIMEOUT, null);
            pluginManager.downloadAndExtract("elasticsearch/license/latest");

            assertThat(requests, hasSize(1));
        } finally {
            server.stop();
        }
    }

    public void testThatProxyCanContainBasicAuthInformation() throws Exception {
        assumeTrue("test requires security manager to be disabled", System.getSecurityManager() == null);

        System.setProperty("http.proxyHost", "localhost");
        System.setProperty("http.proxyPort", "" + randomProxyPort);
        System.setProperty("http.proxyUser", PROXY_AUTH_USERNAME);
        System.setProperty("http.proxyPassword", PROXY_AUTH_PASSWORD);

        HttpProxyServer server = DefaultHttpProxyServer.bootstrap()
                .withPort(randomProxyPort)
                .withFiltersSource(filtersSource)
                .withProxyAuthenticator(proxyAuthenticator)
            .start();

        try {
            PluginManager pluginManager = new PluginManager(environment, null, OutputMode.VERBOSE, DEFAULT_TIMEOUT, null);
            pluginManager.downloadAndExtract("elasticsearch/license/latest");

            assertThat(requests, hasSize(1));
        } finally {
            server.stop();
        }
    }

    public void testThatProxyCanContainBasicAuthInformationViaCommandline() throws Exception {
        assumeTrue("test requires security manager to be disabled", System.getSecurityManager() == null);

        HttpProxyServer server = DefaultHttpProxyServer.bootstrap()
                .withPort(randomProxyPort)
                .withFiltersSource(filtersSource)
                .withProxyAuthenticator(proxyAuthenticator)
                .start();

        try {
            String url = String.format(Locale.ROOT, "http://%s:%s@localhost:%s", PROXY_AUTH_USERNAME, PROXY_AUTH_PASSWORD, randomProxyPort);
            PluginManager pluginManager = new PluginManager(environment, null, OutputMode.VERBOSE, DEFAULT_TIMEOUT, url);
            pluginManager.downloadAndExtract("elasticsearch/license/latest");

            assertThat(requests, hasSize(1));
        } finally {
            server.stop();
        }
    }

    public void testThatPropertiesAreSetCorrectlyFromUrlString() throws Exception {
        for (String protocol : new String[]{"http", "https"}) {
            assertSystemProxyProperties(protocol + "://username:password@proxy.acme.corp:3128", protocol, "proxy.acme.corp", 3128, "username", "password");
            assertSystemProxyProperties(protocol + "://proxy.acme.corp:3128", protocol, "proxy.acme.corp", 3128, null, null);
            assertSystemProxyProperties(protocol + "://proxy.acme.corp", protocol, "proxy.acme.corp", null, null, null);
            assertSystemProxyProperties(protocol + "://username@proxy.acme.corp:3128", protocol, "proxy.acme.corp", 3128, null, null);
            assertSystemProxyProperties(protocol + "://:password@proxy.acme.corp:3128", protocol, "proxy.acme.corp", 3128, "", "password");
            assertSystemProxyProperties(protocol + "://username:@proxy.acme.corp:3128", protocol, "proxy.acme.corp", 3128, "username", "");
        }
        assertSystemProxyProperties(null, null, null, null, null, null);

        try {
            assertSystemProxyProperties("proxy.acme.corp", "http", "proxy.acme.corp", 3128, "username", "password");
            fail("Expected IllegalArgumentException but did not happen");
        } catch (IllegalArgumentException e) {}
    }

    private void assertSystemProxyProperties(String uri, String protocol, String host, Integer port, String username, String password) throws Exception {
        try {
            setProxyPropertiesFromURL(uri);
            assertThat(String.format(Locale.ROOT, "Expected %s.proxyHost to be %s", protocol, host), System.getProperty(protocol + ".proxyHost"), is(host));

            if (port == null) {
                assertThat(String.format(Locale.ROOT, "Expected %s.proxyPort to be null", protocol), System.getProperty(protocol + ".proxyPort"), is(nullValue()));
            } else {
                assertThat(String.format(Locale.ROOT, "Expected %s.proxyPort to be %s", protocol, port), System.getProperty(protocol + ".proxyPort"), is(String.valueOf(port)));
            }

            if (username == null) {
                assertThat(String.format(Locale.ROOT, "Expected %s.proxyUser to be null", protocol, username), System.getProperty(protocol + ".proxyUser"), is(nullValue()));
            } else {
                assertThat(String.format(Locale.ROOT, "Expected %s.proxyUser to be %s", protocol, username), System.getProperty(protocol + ".proxyUser"), is(username));
            }

            if (password == null) {
                assertThat(String.format(Locale.ROOT, "Expected %s.proxyPassword to be null", protocol, password), System.getProperty(protocol + ".proxyPassword"), is(nullValue()));
            } else {
                assertThat(String.format(Locale.ROOT, "Expected %s.proxyPassword to be %s", protocol, password), System.getProperty(protocol + ".proxyPassword"), is(password));
            }
        } finally {
            clearProperties();
        }
    }
}
