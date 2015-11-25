/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;


@ESRestTestCase.Rest
@ClusterScope(scope = SUITE, numClientNodes = 1, transportClientRatio = 0, numDataNodes = 1, randomDynamicTemplates = false)
@TestLogging("_root:DEBUG")
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
            InetSocketAddress address = cluster().httpAddresses()[0];
            HttpPut request = new HttpPut(new URI("http", null, NetworkAddress.formatAddress(address.getAddress()), address.getPort(), "/_watcher/_start", null, null));
            client.execute(request);
        }
    }

    @After
    public void stopWatcher() throws Exception {
        try(CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            InetSocketAddress address = cluster().httpAddresses()[0];
            HttpPut request = new HttpPut(new URI("http", null, NetworkAddress.formatAddress(address.getAddress()), address.getPort(), "/_watcher/_stop", null, null));
            client.execute(request);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, true)
        .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(Node.HTTP_ENABLED, true)
                .build();
    }

}
