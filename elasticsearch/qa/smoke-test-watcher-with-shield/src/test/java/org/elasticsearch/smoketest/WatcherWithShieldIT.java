/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

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
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
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

public class WatcherWithShieldIT extends ESRestTestCase {

    private final static String TEST_ADMIN_USERNAME = "test_admin";
    private final static String TEST_ADMIN_PASSWORD = "changeme";

    public WatcherWithShieldIT(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ESRestTestCase.createParameters(0, 1);
    }

    @Before
    public void startWatcher() throws Exception {
        try(CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            InetSocketAddress address = cluster().httpAddresses()[0];
            HttpPut request = new HttpPut(new URI("http", null, NetworkAddress.formatAddress(address.getAddress()), address.getPort(), "/_watcher/_start", null, null));
            String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecuredString(TEST_ADMIN_PASSWORD.toCharArray()));
            request.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, token);
            client.execute(request);
        }
    }

    @After
    public void stopWatcher() throws Exception {
        try(CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            InetSocketAddress address = cluster().httpAddresses()[0];
            HttpPut request = new HttpPut(new URI("http", null, NetworkAddress.formatAddress(address.getAddress()), address.getPort(), "/_watcher/_stop", null, null));
            String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecuredString(TEST_ADMIN_PASSWORD.toCharArray()));
            request.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, token);
            client.execute(request);
        }
    }

    @Override
    protected Settings restClientSettings() {
        String[] credentials = getCredentials();
        String token = basicAuthHeaderValue(credentials[0], new SecuredString(credentials[1].toCharArray()));
        return Settings.builder()
                .put(Headers.PREFIX + ".Authorization", token)
                .build();
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
                .put("shield.user", TEST_ADMIN_USERNAME + ":" + TEST_ADMIN_PASSWORD)
                .build();
    }

    protected String[] getCredentials() {
        return new String[]{"watcher_manager", "changeme"};
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.<Class<? extends Plugin>>singleton(XPackPlugin.class);
    }

}

