/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.apache.http.client.methods.HttpRequestBase;

import java.net.URI;

public interface Request {
    HttpRequestBase createRequest();

    URI getURI();

    /**
     * Create a new request with less input text.
     * @return a new {@link Request} with the truncated input text
     */
    Request truncate();

    /**
     * Returns an array of booleans indicating if the text input at that same array index was truncated in the request
     * sent to the 3rd party server.
     */
    boolean[] getTruncationInfo();
}
