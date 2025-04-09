/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

public class CohereUtils {
    public static final String HOST = "api.cohere.ai";
    public static final String VERSION_1 = "v1";
    public static final String CHAT_PATH = "chat";
    public static final String EMBEDDINGS_PATH = "embed";
    public static final String RERANK_PATH = "rerank";
    public static final String REQUEST_SOURCE_HEADER = "Request-Source";
    public static final String ELASTIC_REQUEST_SOURCE = "unspecified:elasticsearch";

    public static Header createRequestSourceHeader() {
        return new BasicHeader(REQUEST_SOURCE_HEADER, ELASTIC_REQUEST_SOURCE);
    }

    private CohereUtils() {}
}
