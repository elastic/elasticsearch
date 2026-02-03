/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;

public class MixedbreadRerankResponseHandler extends MixedbreadResponseHandler {
    /**
     * Constructs a new MixedbreadRerankResponseHandler with the specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public MixedbreadRerankResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction);
    }
}
