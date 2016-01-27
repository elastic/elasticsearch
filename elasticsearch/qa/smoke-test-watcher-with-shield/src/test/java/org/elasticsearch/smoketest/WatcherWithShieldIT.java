/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.junit.After;
import org.junit.Before;

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
            URL url = getClusterUrls()[0];
            HttpPut request = new HttpPut(new URI("http", null, url.getHost(), url.getPort(), "/_watcher/_start", null, null));
            String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecuredString(TEST_ADMIN_PASSWORD.toCharArray()));
            request.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, token);
            client.execute(request);
        }
    }

    @After
    public void stopWatcher() throws Exception {
        try(CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            URL url = getClusterUrls()[0];
            HttpPut request = new HttpPut(new URI("http", null, url.getHost(), url.getPort(), "/_watcher/_stop", null, null));
            String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecuredString(TEST_ADMIN_PASSWORD.toCharArray()));
            request.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, token);
            client.execute(request);
        }
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("watcher_manager", new SecuredString("changeme".toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecuredString(TEST_ADMIN_PASSWORD.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }
}

