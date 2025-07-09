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
import org.elasticsearch.xpack.esql.inference.InferenceRunner;

public record TransportActionServices(
    TransportService transportService,
    SearchService searchService,
    ExchangeService exchangeService,
    ClusterService clusterService,
    ProjectResolver projectResolver,
    IndexNameExpressionResolver indexNameExpressionResolver,
    UsageService usageService,
    InferenceRunner inferenceRunner
) {}
