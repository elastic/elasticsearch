/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4.authenticate;

import io.netty.channel.Channel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.http.HttpPreRequest;

public interface HttpAuthenticator {
    /**
     * An async HTTP headers authenticating function that receives as arguments part of the incoming HTTP request
     * (except the body contents, see {@link HttpPreRequest}), as well as the netty channel that the request is
     * being received over, and must then call the {@code ActionListener#onResponse} method on the listener parameter
     * in case the authentication is to be considered successful, or otherwise call {@code ActionListener#onFailure}
     * and pass the failure exception.
     */
    void authenticate(HttpPreRequest httpPreRequest, Channel channel, ActionListener<Void> listener);
}
