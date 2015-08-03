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

import com.google.common.base.Charsets;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.PluginManager.OutputMode;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.plugins.PluginManager.setProxyPropertiesFromURL;
import static org.elasticsearch.plugins.PluginManagerCliParser.DEFAULT_TIMEOUT;
import static org.hamcrest.Matchers.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@LuceneTestCase.SuppressFileSystems("*") // due to jimfs, see PluginManagerTests
public class PluginManagerProxyTests extends ElasticsearchTestCase {

    public static final String PROXY_AUTH_USERNAME = "mySecretUser";
    public static final String PROXY_AUTH_PASSWORD = "mySecretPassword";

    private final List<HttpRequest> requests = new ArrayList<>();
    private Environment environment;

    private ServerBootstrap serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory());
    private ChannelPipelineFactory channelPipelineFactory = new ChannelPipelineFactory() {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(
                    new HttpRequestDecoder(),
                    new HttpResponseEncoder(),
                    new LoggingServerHandler(requests)
            );
        }
    };
    private int boundPort;

    @Before
    public void setupEnvironment() throws Exception {
        Path homeDir = createTempDir();
        Files.createDirectory(homeDir.resolve("config"));
        Files.createDirectory(homeDir.resolve("plugins"));
        System.setProperty("es.path.home", homeDir.toString());
        environment = InternalSettingsPreparer.prepareSettings(EMPTY_SETTINGS, true, Terminal.DEFAULT).v2();

        serverBootstrap.setPipelineFactory(channelPipelineFactory);
        Channel channel = serverBootstrap.bind(new InetSocketAddress("localhost", 0));
        this.boundPort = ((InetSocketAddress) channel.getLocalAddress()).getPort();
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

        serverBootstrap.releaseExternalResources();
    }

    public void testThatProxyCanBeConfiguredViaProperty() throws Exception {
        assumeTrue("test requires security manager to be disabled to change the default authenticator", System.getSecurityManager() == null);

        System.setProperty("proxyHost", "localhost");
        System.setProperty("proxyPort", "" + boundPort);

        PluginManager pluginManager = new PluginManager(environment, null, OutputMode.VERBOSE, DEFAULT_TIMEOUT, null);
        try {
            pluginManager.downloadAndExtract("elasticsearch/license/latest", new CliToolTestCase.MockTerminal());
        } catch (IOException e) {}

        // no auth info, but if the request hit the netty started as proxy, we are already happy
        assertThat(requests, hasSize(greaterThan(0)));
    }

    public void testThatProxyCanContainBasicAuthInformation() throws Exception {
        assumeTrue("test requires security manager to be disabled to change the default authenticator", System.getSecurityManager() == null);

        System.setProperty("http.proxyHost", "localhost");
        System.setProperty("http.proxyPort", "" + boundPort);
        System.setProperty("http.proxyUser", PROXY_AUTH_USERNAME);
        System.setProperty("http.proxyPassword", PROXY_AUTH_PASSWORD);

        PluginManager pluginManager = new PluginManager(environment, null, OutputMode.VERBOSE, DEFAULT_TIMEOUT, null);
        try {
            pluginManager.downloadAndExtract("elasticsearch/license/latest", new CliToolTestCase.MockTerminal());
        } catch (IOException e) {}

        assertAnyRequestContainsProxyAuthorzationHeader();
    }

    public void testThatProxyCanContainBasicAuthInformationViaCommandline() throws Exception {
        assumeTrue("test requires security manager to be disabled to change the default authenticator", System.getSecurityManager() == null);

        String proxy = String.format(Locale.ROOT, "http://%s:%s@localhost:%s", PROXY_AUTH_USERNAME, PROXY_AUTH_PASSWORD, boundPort);
        PluginManager pluginManager = new PluginManager(environment, null, OutputMode.DEFAULT, DEFAULT_TIMEOUT, proxy);
        try {
            pluginManager.downloadAndExtract("elasticsearch/license/latest", new CliToolTestCase.MockTerminal());
        } catch (IOException e) {}

        assertAnyRequestContainsProxyAuthorzationHeader();
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

    private void assertAnyRequestContainsProxyAuthorzationHeader() {
        assertThat(requests, hasSize(greaterThan(1)));

        String data = PROXY_AUTH_USERNAME + ":" + PROXY_AUTH_PASSWORD;
        String base64UserPass = Base64.encodeBytes(data.getBytes(Charsets.UTF_8));
        // any of the requests should have the proxy auth set
        boolean found = false;
        for (HttpRequest request : requests) {
            if (request.headers().contains("Proxy-Authorization") && request.headers().get("Proxy-Authorization").equals("Basic " + base64UserPass)) {
                found = true;
                break;
            }
        }
        assertThat("No logged request contained proxy-authorization header", found, is(true));
    }

    private static class LoggingServerHandler extends SimpleChannelUpstreamHandler {

        private List<HttpRequest> requests;

        public LoggingServerHandler(List<HttpRequest> requests) {
            this.requests = requests;
        }

        @Override
        public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws InterruptedException {
            final HttpRequest request = (HttpRequest) e.getMessage();
            requests.add(request);

            final org.jboss.netty.handler.codec.http.HttpResponse response;
            if (!request.headers().contains("Proxy-Authorization")) {
                response = new DefaultHttpResponse(HTTP_1_1, PROXY_AUTHENTICATION_REQUIRED);
                response.headers().add(HttpHeaders.Names.PROXY_AUTHENTICATE, "Basic realm=\"Proxy\"");
                response.headers().add(HttpHeaders.Names.CONTENT_TYPE, "text/html");
                response.setContent(ChannelBuffers.wrappedBuffer("ADD PROXY AUTH INFOS".getBytes(Charsets.UTF_8)));
            } else {
                response = new DefaultHttpResponse(HTTP_1_1, BAD_REQUEST);
            }
            ctx.getChannel().write(response);
        }
    }
}
