/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.network.CloseableChannel;

import java.net.InetSocketAddress;

public interface HttpServerChannel extends CloseableChannel {

    /**
     * Returns the local address for this channel.
     *
     * @return the local address of this channel.
     */
    InetSocketAddress getLocalAddress();
}
