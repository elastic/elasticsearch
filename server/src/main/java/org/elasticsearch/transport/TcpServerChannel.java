/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.network.CloseableChannel;

import java.net.InetSocketAddress;

/**
 * This is a tcp channel representing a server channel listening for new connections. It is the server
 * channel abstraction used by the {@link TcpTransport} and {@link TransportService}. All tcp transport
 * implementations must return server channels that adhere to the required method contracts.
 */
public interface TcpServerChannel extends CloseableChannel {

    /**
     * Returns the local address for this channel.
     *
     * @return the local address of this channel.
     */
    InetSocketAddress getLocalAddress();

}
