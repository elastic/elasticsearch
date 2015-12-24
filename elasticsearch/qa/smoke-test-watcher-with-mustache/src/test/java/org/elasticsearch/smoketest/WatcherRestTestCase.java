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
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

public abstract class WatcherRestTestCase extends ESRestTestCase {

    public WatcherRestTestCase(@Name("yaml") RestTestCandidate testCandidate) {
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
            client.execute(request);
        }
    }

    @After
    public void stopWatcher() throws Exception {
        try(CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            URL url = getClusterUrls()[0];
            HttpPut request = new HttpPut(new URI("http", null, url.getHost(), url.getPort(), "/_watcher/_stop", null, null));
            client.execute(request);
        }
    }
}
