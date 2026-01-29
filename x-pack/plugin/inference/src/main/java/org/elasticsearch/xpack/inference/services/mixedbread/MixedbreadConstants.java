/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.apache.http.client.utils.URIBuilder;

public class MixedbreadConstants {
    public static final String HOST = "api.mixedbread.com";
    public static final String VERSION_1 = "v1";
    public static final String RERANK_PATH = "rerank";
    public static URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https").setHost(MixedbreadConstants.HOST);

    // common service settings fields
    public static final String MODEL_FIELD = "model";

    public static final String INPUT_FIELD = "input";

    // rerank task settings fields
    public static final String QUERY_FIELD = "query";

    public static final String DOCUMENTS_FIELD = "documents";

    // rerank task settings fields
    public static final String RETURN_DOCUMENTS_FIELD = "return_input";
    public static final String TOP_K_FIELD = "top_k";

    private MixedbreadConstants() {}
}
