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
     * @param reductionPercentage the percent to reduce the input text by (e.g. 0.5)
     * @return a new {@link Request} with the truncated input text
     */
    Request truncate(double reductionPercentage);
}
