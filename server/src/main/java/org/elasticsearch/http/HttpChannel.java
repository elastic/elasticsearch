/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.network.CloseableChannel;

import java.net.InetSocketAddress;

public interface HttpChannel extends CloseableChannel {

    /**
     * Sends an http response to the channel. The listener will be executed once the send process has been
     * completed.
     *
     * @param response to send to channel
     * @param listener to execute upon send completion
     */
    void sendResponse(HttpResponse response, ActionListener<Void> listener);

    /**
     * Returns the local address for this channel.
     *
     * @return the local address of this channel.
     */
    InetSocketAddress getLocalAddress();

    /**
     * Returns the remote address for this channel. Can be null if channel does not have a remote address.
     *
     * @return the remote address of this channel.
     */
    InetSocketAddress getRemoteAddress();

}
