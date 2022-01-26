/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    private volatile Set<HttpHost> httpHosts = new HashSet<>();

    @Override
    public void onFailure(Node node) {
        httpHosts.add(node.getHost());
    }

    void assertCalled(List<Node> nodes) {
        HttpHost[] hosts = new HttpHost[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            hosts[i] = nodes.get(i).getHost();
        }
        assertCalled(hosts);
    }

    void assertCalled(HttpHost... hosts) {
        assertEquals(hosts.length, this.httpHosts.size());
        assertThat(this.httpHosts, containsInAnyOrder(hosts));
        this.httpHosts.clear();
    }

    void assertNotCalled() {
        assertEquals(0, httpHosts.size());
    }
}
