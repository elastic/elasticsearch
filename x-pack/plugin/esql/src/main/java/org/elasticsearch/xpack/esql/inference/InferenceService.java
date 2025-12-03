/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRunner;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRunnerConfig;

public class InferenceService {
    private final InferenceResolver.Factory inferenceResolverFactory;

    private final BulkInferenceRunner.Factory bulkInferenceRunnerFactory;

    private final InferenceSettings inferenceSettings;

    /**
     * Creates a new inference service with the given client.
     *
     * @param client the Elasticsearch client for inference operations
     */
    public InferenceService(Client client, Settings settings) {
        this(InferenceResolver.factory(client), BulkInferenceRunner.factory(client), settings);
    }

    private InferenceService(
        InferenceResolver.Factory inferenceResolverFactory,
        BulkInferenceRunner.Factory bulkInferenceRunnerFactory,
        Settings settings
    ) {
        this.inferenceResolverFactory = inferenceResolverFactory;
        this.bulkInferenceRunnerFactory = bulkInferenceRunnerFactory;
        this.inferenceSettings = InferenceSettings.fromSettings(settings);
    }

    /**
     * Creates an inference resolver for resolving inference IDs in logical plans.
     *
     * @param functionRegistry the function registry to resolve functions
     *
     * @return a new inference resolver instance
     */
    public InferenceResolver inferenceResolver(EsqlFunctionRegistry functionRegistry) {
        return inferenceResolverFactory.create(functionRegistry);
    }

    /**
     * Returns the inference configuration settings.
     *
     * @return the inference settings
     */
    public InferenceSettings inferenceSettings() {
        return inferenceSettings;
    }

    public BulkInferenceRunner bulkInferenceRunner() {
        return bulkInferenceRunner(BulkInferenceRunnerConfig.DEFAULT);
    }

    public BulkInferenceRunner bulkInferenceRunner(BulkInferenceRunnerConfig bulkInferenceRunnerConfig) {
        return bulkInferenceRunnerFactory.create(bulkInferenceRunnerConfig);
    }
}
