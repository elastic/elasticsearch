/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;


@ESRestTestCase.Rest
@ClusterScope(scope = SUITE, numClientNodes = 1, transportClientRatio = 0, numDataNodes = 1, randomDynamicTemplates = false)
@TestLogging("_root:DEBUG")
public abstract class XPackRestTestCase extends ESRestTestCase {

    public XPackRestTestCase(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ESRestTestCase.createParameters(0, 1);
    }

    @Before
    public void startWatcher() throws Exception {
        try(CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            if (cluster() == null || cluster().httpAddresses() == null) {
                fail("no address available to start watcher");
            }
            InetSocketAddress address = cluster().httpAddresses()[0];
            HttpPut request = new HttpPut(new URI("http",
                                                  "test_user:changeme",
                                                  NetworkAddress.formatAddress(address.getAddress()),
                                                  address.getPort(),
                                                  "/_watcher/start", null, null));
            client.execute(request);
        }
    }

    @After
    public void stopWatcher() throws Exception {
        try(CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            if (cluster() == null || cluster().httpAddresses() == null) {
                fail("no address available to stop watcher");
            }
            InetSocketAddress address = cluster().httpAddresses()[0];
            HttpPut request = new HttpPut(new URI("http",
                                                  "test_user:changeme",
                                                  NetworkAddress.formatAddress(address.getAddress()),
                                                  address.getPort(),
                                                  "/_watcher/stop", null, null));
            client.execute(request);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, true)
                .put("shield.user", "test_user:changeme")
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(Node.HTTP_ENABLED, true)
                .put("shield.user", "test_user:changeme")
                .build();
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
                .put(Node.HTTP_ENABLED, true)
                .put("shield.user", "test_user:changeme")
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(XPackPlugin.class);
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_user", new SecuredString("changeme".toCharArray()));
        return Settings.builder()
                .put(Headers.PREFIX + ".Authorization", token)
                .build();
    }

}
