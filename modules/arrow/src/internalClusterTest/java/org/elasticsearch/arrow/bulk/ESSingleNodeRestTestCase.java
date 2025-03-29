/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.net.InetSocketAddress;

/**
 * An {@link ESSingleNodeTestCase} with a Rest client (a feature that is provided by {@code ESIntegTestCase}).
 */
public abstract class ESSingleNodeRestTestCase extends ESSingleNodeTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public RestClient createRestClient() {
        NodeService instance = node().injector().getInstance(NodeService.class);
        var httpInfo = instance.info(false, false, false, false, false, false, true, false, false, false, false, false)
            .getInfo(HttpInfo.class);

        assertNotNull("Couldn't get the node's http info", httpInfo);

        InetSocketAddress address = httpInfo.address().publishAddress().address();
        HttpHost host = new HttpHost(NetworkAddress.format(address.getAddress()), address.getPort(), "http");
        return RestClient.builder(host).build();
    }
}
