/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

public class AzureOpenAiUtils {

    public static final String HOST_SUFFIX = "openai.azure.com";
    public static final String OPENAI_PATH = "openai";
    public static final String DEPLOYMENTS_PATH = "deployments";
    public static final String EMBEDDINGS_PATH = "embeddings";
    public static final String CHAT_PATH = "chat";
    public static final String COMPLETIONS_PATH = "completions";
    public static final String API_VERSION_PARAMETER = "api-version";
    public static final String API_KEY_HEADER = "api-key";

    private AzureOpenAiUtils() {}
}
