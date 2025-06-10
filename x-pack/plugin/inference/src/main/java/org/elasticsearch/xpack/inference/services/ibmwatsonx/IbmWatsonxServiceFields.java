/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

public class IbmWatsonxServiceFields {

    /**
     * Taken from <a href="https://cloud.ibm.com/apidocs/watsonx-ai#text-embeddings">
     *     Introduction to IBM Watsonx.ai as a Service</a>
     */
    static final int EMBEDDING_MAX_BATCH_SIZE = 1000;
    public static final String API_VERSION = "api_version";

    public static final String PROJECT_ID = "project_id";
}
