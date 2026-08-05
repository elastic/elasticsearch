/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.voyageai.action.VoyageAIActionVisitor;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class VoyageAIModel extends RateLimitGroupingModel {
    private static final String DEFAULT_MODEL_FAMILY = "default_model_family";
    private static final Map<String, String> MODEL_TO_MODEL_FAMILY;

    static {
        Map<String, String> tempMap = new HashMap<>();
        tempMap.put("voyage-3.5", "embed_medium");
        tempMap.put("voyage-3.5-lite", "embed_small");
        tempMap.put("voyage-multimodal-3", "embed_multimodal");
        tempMap.put("voyage-3-large", "embed_large");
        tempMap.put("voyage-code-3", "embed_large");
        tempMap.put("voyage-3", "embed_medium");
        tempMap.put("voyage-3-lite", "embed_small");
        tempMap.put("voyage-finance-2", "embed_large");
        tempMap.put("voyage-law-2", "embed_large");
        tempMap.put("voyage-code-2", "embed_large");
        tempMap.put("rerank-2", "rerank_large");
        tempMap.put("rerank-2-lite", "rerank_small");

        MODEL_TO_MODEL_FAMILY = Collections.unmodifiableMap(tempMap);
    }

    private final URI uri;

    public VoyageAIModel(ModelConfigurations configurations, ModelSecrets secrets, URI uri) {
        super(configurations, secrets);
        this.uri = Objects.requireNonNull(uri);
    }

    protected VoyageAIModel(VoyageAIModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        this.uri = model.uri;
    }

    protected VoyageAIModel(VoyageAIModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        this.uri = model.uri;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    public int rateLimitGroupingHash() {
        String modelId = getServiceSettings().modelId();
        String modelFamily = MODEL_TO_MODEL_FAMILY.getOrDefault(modelId, DEFAULT_MODEL_FAMILY);

        return Objects.hash(modelFamily, getSecretSettings().apiKey());
    }

    @Override
    public VoyageAIServiceSettings getServiceSettings() {
        return (VoyageAIServiceSettings) super.getServiceSettings();
    }

    public URI uri() {
        return uri;
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public abstract ExecutableAction accept(VoyageAIActionVisitor creator, Map<String, Object> taskSettings);
}
