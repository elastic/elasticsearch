/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public abstract class DiscoveryEc2NetworkAddressesTestCase extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), Ec2DiscoveryPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    void verifyPublishAddress(String publishAddressSetting, String expectedAddress) throws IOException {
        final var node = internalCluster().startNode(Settings.builder().put("http.publish_host", publishAddressSetting));
        assertEquals(
            expectedAddress,
            internalCluster().getInstance(HttpServerTransport.class, node).boundAddress().publishAddress().getAddress()
        );
        internalCluster().stopNode(node);
    }
}
