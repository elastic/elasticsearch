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

package org.elasticsearch.test;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.plugins.Plugin;

/**
 * A dummy http transport used by tests when not wanting to actually bind to a real address.
 */
public class MockHttpTransport extends AbstractLifecycleComponent implements HttpServerTransport {

    /**
     * Marker plugin used by {@link org.elasticsearch.node.MockNode} to enable {@link MockHttpTransport}.
     */
    public static class TestPlugin extends Plugin {}

    // dummy address/info that can be read by code expecting objects from the relevant methods,
    // but not actually used for a real connection
    private static final TransportAddress DUMMY_TRANSPORT_ADDRESS = new TransportAddress(TransportAddress.META_ADDRESS, 0);
    private static final BoundTransportAddress DUMMY_BOUND_ADDRESS = new BoundTransportAddress(
        new TransportAddress[] { DUMMY_TRANSPORT_ADDRESS }, DUMMY_TRANSPORT_ADDRESS);
    private static final HttpInfo DUMMY_HTTP_INFO = new HttpInfo(DUMMY_BOUND_ADDRESS, 0);
    private static final HttpStats DUMMY_HTTP_STATS = new HttpStats(0, 0);

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

    @Override
    public BoundTransportAddress boundAddress() {
        return DUMMY_BOUND_ADDRESS;
    }

    @Override
    public HttpInfo info() {
        return DUMMY_HTTP_INFO;
    }

    @Override
    public HttpStats stats() {
        return DUMMY_HTTP_STATS;
    }
}
