/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceResolver;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.inference.InferenceRunnerConfig;

public class TransportActionServices {
    private final TransportService transportService;
    private final SearchService searchService;
    private final ExchangeService exchangeService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final UsageService usageService;
    private final InferenceResolver.Factory inferenceResolverFactory;
    private final InferenceRunner.Factory inferenceRunnerFactory;

    public TransportActionServices(
        TransportService transportService,
        SearchService searchService,
        ExchangeService exchangeService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        UsageService usageService,
        InferenceResolver.Factory inferenceResolverFactory,
        InferenceRunner.Factory inferenceRunnerFactory
    ) {
        this.transportService = transportService;
        this.searchService = searchService;
        this.exchangeService = exchangeService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.usageService = usageService;
        this.inferenceResolverFactory = inferenceResolverFactory;
        this.inferenceRunnerFactory = inferenceRunnerFactory;
    }

    public TransportService transportService() {
        return transportService;
    }

    public SearchService searchService() {
        return searchService;
    }

    public ExchangeService exchangeService() {
        return exchangeService;
    }

    public ClusterService clusterService() {
        return clusterService;
    }

    public ProjectResolver projectResolver() {
        return projectResolver;
    }

    public IndexNameExpressionResolver indexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    public UsageService usageService() {
        return usageService;
    }

    /**
     * Creates an inference resolver for collecting inference IDs from logical plans.
     *
     * @param functionRegistry The function registry for resolving inference functions
     * @return A resolver capable of extracting inference IDs from plans and expressions
     */
    public InferenceResolver inferenceResolver(EsqlFunctionRegistry functionRegistry) {
        return inferenceResolverFactory.create(functionRegistry);
    }

    /**
     * Returns the inference runner factory for creating multiple configured runners.
     *
     * @return The factory for creating inference runners
     */
    public InferenceRunner.Factory inferenceRunnerFactory() {
        return inferenceRunnerFactory;
    }

    /**
     * Creates an inference runner with the specified execution configuration using the default configuration.
     *
     * @return A configured inference runner capable of executing inference requests
     */
    public InferenceRunner inferenceRunner() {
        return inferenceRunner(InferenceRunnerConfig.DEFAULT);
    }

    /**
     * Creates an inference runner with the specified execution configuration.
     *
     * @param inferenceRunnerConfig Configuration specifying concurrency limits and execution parameters
     * @return A configured inference runner capable of executing inference requests
     */
    public InferenceRunner inferenceRunner(InferenceRunnerConfig inferenceRunnerConfig) {
        return inferenceRunnerFactory.create(inferenceRunnerConfig);
    }
}
