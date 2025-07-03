/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.inference.InputType;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

public class CohereUtils {
    public static final String HOST = "api.cohere.ai";
    public static final String VERSION_1 = "v1";
    public static final String VERSION_2 = "v2";
    public static final String CHAT_PATH = "chat";
    public static final String EMBEDDINGS_PATH = "embed";
    public static final String RERANK_PATH = "rerank";
    public static final String REQUEST_SOURCE_HEADER = "Request-Source";
    public static final String ELASTIC_REQUEST_SOURCE = "unspecified:elasticsearch";

    public static final String CLUSTERING = "clustering";
    public static final String CLASSIFICATION = "classification";
    public static final String DOCUMENTS_FIELD = "documents";
    public static final String EMBEDDING_TYPES_FIELD = "embedding_types";
    public static final String INPUT_TYPE_FIELD = "input_type";
    public static final String MESSAGE_FIELD = "message";
    public static final String MODEL_FIELD = "model";
    public static final String QUERY_FIELD = "query";
    public static final String SEARCH_DOCUMENT = "search_document";
    public static final String SEARCH_QUERY = "search_query";
    public static final String TEXTS_FIELD = "texts";
    public static final String STREAM_FIELD = "stream";

    public static Header createRequestSourceHeader() {
        return new BasicHeader(REQUEST_SOURCE_HEADER, ELASTIC_REQUEST_SOURCE);
    }

    public static String inputTypeToString(InputType inputType) {
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> SEARCH_DOCUMENT;
            case SEARCH, INTERNAL_SEARCH -> SEARCH_QUERY;
            case CLASSIFICATION -> CLASSIFICATION;
            case CLUSTERING -> CLUSTERING;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }

    private CohereUtils() {}
}
