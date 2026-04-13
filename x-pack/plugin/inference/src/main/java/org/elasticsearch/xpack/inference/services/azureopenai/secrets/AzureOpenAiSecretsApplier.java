/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.action.ActionListener;

/**
 * An interface for applying secrets to Azure OpenAI requests.
 */
public interface AzureOpenAiSecretsApplier {
    void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener);
}
