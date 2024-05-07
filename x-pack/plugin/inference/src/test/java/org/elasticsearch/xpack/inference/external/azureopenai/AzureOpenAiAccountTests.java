/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.azureopenai;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModelTests;

import static org.hamcrest.Matchers.is;

public class AzureOpenAiAccountTests extends ESTestCase {

    public void testFromAzureOpenAiEmbeddingsModel() {
        AzureOpenAiEmbeddingsModel model = AzureOpenAiEmbeddingsModelTests.createModelWithRandomValues();
        AzureOpenAiAccount account = AzureOpenAiAccount.fromModel(model);

        assertThat(account.apiKey(), is(model.getSecretSettings().apiKey()));
        assertThat(account.apiVersion(), is(model.getServiceSettings().apiVersion()));
        assertThat(account.entraId(), is(model.getSecretSettings().entraId()));
        assertThat(account.deploymentId(), is(model.getServiceSettings().deploymentId()));
        assertThat(account.resourceName(), is(model.getServiceSettings().resourceName()));
    }

}
