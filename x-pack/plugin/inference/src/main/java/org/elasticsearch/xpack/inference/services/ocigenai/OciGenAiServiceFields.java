/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

/**
 * Setting names and constants shared by the OCI Generative AI service implementation.
 */
public final class OciGenAiServiceFields {

    /** The OCI region identifier the inference endpoint lives in, for example {@code us-chicago-1}. */
    public static final String REGION = "region";
    /** The OCID of the compartment that the requests are billed to and authorized against. */
    public static final String COMPARTMENT_ID = "compartment_id";
    /** Optional OCID of a dedicated AI cluster endpoint. When set, requests use the {@code DEDICATED} serving mode. */
    public static final String ENDPOINT_ID = "endpoint_id";

    /** Embeddings task setting: the OCI input type to embed for (search document, search query, classification, clustering). */
    public static final String INPUT_TYPE = "input_type";
    /** Embeddings task setting: how the service truncates inputs that exceed the model's maximum token count. */
    public static final String TRUNCATE = "truncate";

    /** Rerank task setting: the number of top ranked documents to return. */
    public static final String TOP_N = "top_n";
    /** Rerank task setting: whether the ranked documents' text is returned alongside the scores. */
    public static final String RETURN_DOCUMENTS = "return_documents";

    /**
     * The OCI Generative AI {@code embedText} action accepts at most 96 inputs per request for the Cohere embedding models,
     * see <a href="https://docs.oracle.com/en-us/iaas/api/#/en/generative-ai-inference/20231130/datatypes/EmbedTextDetails">EmbedTextDetails</a>.
     */
    public static final int EMBEDDING_MAX_BATCH_SIZE = 96;

    private OciGenAiServiceFields() {}
}
