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
import org.elasticsearch.xpack.esql.inference.InferenceResolver;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRunner;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRunnerConfig;

public class TransportActionServices {
    private final TransportService transportService;
    private final SearchService searchService;
    private final ExchangeService exchangeService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final UsageService usageService;
    private final InferenceResolver.Factory inferenceResolverFactory;
    private final BulkInferenceRunner.Factory inferenceRunnerFactory;

    public TransportActionServices(
        TransportService transportService,
        SearchService searchService,
        ExchangeService exchangeService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        UsageService usageService,
        InferenceResolver.Factory inferenceResolverFactory,
        BulkInferenceRunner.Factory bulkInferenceRunnerFactory
    ) {
        this.transportService = transportService;
        this.searchService = searchService;
        this.exchangeService = exchangeService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.usageService = usageService;
        this.inferenceResolverFactory = inferenceResolverFactory;
        this.inferenceRunnerFactory = bulkInferenceRunnerFactory;
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
     * Creates an inference resolver for collecting used inference IDs from logical plans.
     *
     * @return A resolver capable of extracting inference IDs from plans and expressions
     */
    public InferenceResolver inferenceResolver() {
        return inferenceResolverFactory.create();
    }

    /**
     * Returns the inference runner factory for creating multiple configured runners.
     *
     * @return The factory for creating inference runners
     */
    public BulkInferenceRunner.Factory bulkInferenceRunnerFactory() {
        return inferenceRunnerFactory;
    }

    /**
     * Creates an inference runner with the specified execution configuration using the default configuration.
     *
     * @return A configured inference runner capable of executing inference requests
     */
    public BulkInferenceRunner bulkInferenceRunner() {
        return bulkInferenceRunner(BulkInferenceRunnerConfig.DEFAULT);
    }

    /**
     * Creates an inference runner with the specified execution configuration.
     *
     * @param bulkInferenceRunnerConfig Configuration specifying concurrency limits and execution parameters
     * @return A configured inference runner capable of executing inference requests
     */
    public BulkInferenceRunner bulkInferenceRunner(BulkInferenceRunnerConfig bulkInferenceRunnerConfig) {
        return inferenceRunnerFactory.create(bulkInferenceRunnerConfig);
    }
}
