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
package org.elasticsearch.http.netty3;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty3Plugin;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

import static org.elasticsearch.http.netty3.Netty3HttpClient.returnOpaqueIds;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class Netty3PipeliningEnabledIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(Netty3Plugin.class);
    }

    public void testThatNettyHttpServerSupportsPipelining() throws Exception {
        String[] requests = new String[]{"/", "/_nodes/stats", "/", "/_cluster/state", "/"};

        InetSocketAddress inetSocketAddress = randomFrom(cluster().httpAddresses());
        try (Netty3HttpClient nettyHttpClient = new Netty3HttpClient()) {
            Collection<HttpResponse> responses = nettyHttpClient.get(inetSocketAddress, requests);
            assertThat(responses, hasSize(5));

            Collection<String> opaqueIds = returnOpaqueIds(responses);
            assertOpaqueIdsInOrder(opaqueIds);
        }
    }

    private void assertOpaqueIdsInOrder(Collection<String> opaqueIds) {
        // check if opaque ids are monotonically increasing
        int i = 0;
        String msg = String.format(Locale.ROOT, "Expected list of opaque ids to be monotonically increasing, got [%s]", opaqueIds);
        for (String opaqueId : opaqueIds) {
            assertThat(msg, opaqueId, is(String.valueOf(i++)));
        }
    }

}
