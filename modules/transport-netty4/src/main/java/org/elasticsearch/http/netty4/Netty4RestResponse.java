/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.HttpMessage;

import org.elasticsearch.http.HttpResponse;

public interface Netty4RestResponse extends HttpResponse, HttpMessage {

    int getSequence();

    @Override
    default void addHeader(String name, String value) {
        headers().add(name, value);
    }

    @Override
    default boolean containsHeader(String name) {
        return headers().contains(name);
    }
}
