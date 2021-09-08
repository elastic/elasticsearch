/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
