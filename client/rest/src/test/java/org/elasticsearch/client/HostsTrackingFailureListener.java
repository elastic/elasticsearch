/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.HttpHost;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * {@link RestClient.FailureListener} impl that allows to track when it gets called for which host.
 */
class HostsTrackingFailureListener extends RestClient.FailureListener {
    private volatile Set<HttpHost> hosts = new HashSet<>();

    @Override
    public void onFailure(Node node) {
        hosts.add(node.getHost());
    }

    void assertCalled(List<Node> nodes) {
        HttpHost[] hosts = new HttpHost[nodes.size()];
        for (int i = 0 ; i < nodes.size(); i++) {
            hosts[i] = nodes.get(i).getHost();
        }
        assertCalled(hosts);
    }

    void assertCalled(HttpHost... hosts) {
        assertEquals(hosts.length, this.hosts.size());
        assertThat(this.hosts, containsInAnyOrder(hosts));
        this.hosts.clear();
    }

    void assertNotCalled() {
        assertEquals(0, hosts.size());
    }
}
