/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4.internal;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpRequest;

import org.elasticsearch.action.ActionListener;

public interface HttpValidator {
    void validate(HttpRequest httpRequest, Channel channel, ActionListener<Void> listener);
}
