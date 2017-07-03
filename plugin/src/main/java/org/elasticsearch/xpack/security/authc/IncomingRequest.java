/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import java.net.InetSocketAddress;

/**
 * This represents an incoming request that needs to be authenticated
 */
public interface IncomingRequest {

    /**
     * This method returns the remote address for the request. It will be null if the request is a
     * local transport request.
     *
     * @return the remote socket address
     */
    InetSocketAddress getRemoteAddress();

    /**
     * This returns the type of request that is incoming. It can be a rest request, a remote
     * transport request, or a local transport request.
     *
     * @return the request type
     */
    RequestType getType();

    enum RequestType {
        REST,
        REMOTE_NODE,
        LOCAL_NODE
    }
}
