/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.querysampling.estimate.RecallEstimator;
import org.elasticsearch.xpack.querysampling.storage.StoredSample;
import org.elasticsearch.xpack.querysampling.storage.StoredSamples;

import java.util.List;
import java.util.OptionalDouble;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/**
 * Reads the stored sampled queries that have their ground truth, as the plugin since nobody else may read that
 * index, and estimates the recall from them. Which node did it does not matter, the sample is in the index and
 * not on the node that picked it.
 */
public final class TransportQuerySamplingRecallAction extends TransportAction<QuerySamplingRecallRequest, QuerySamplingRecallResponse> {

    private final StoredSamples storedSamples;

    @Inject
    public TransportQuerySamplingRecallAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(QuerySamplingRecallAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.storedSamples = new StoredSamples(new OriginSettingClient(client, QUERY_SAMPLING_ORIGIN)::search, xContentRegistry);
    }

    @Override
    protected void doExecute(Task task, QuerySamplingRecallRequest request, ActionListener<QuerySamplingRecallResponse> listener) {
        storedSamples.read(
            QueryBuilders.termQuery("has_ground_truth", true),
            "picked_at",
            SortOrder.DESC,
            request.max(),
            listener.map(read -> response(read.samples(), request.includeSamples()))
        );
    }

    private static QuerySamplingRecallResponse response(List<StoredSample> samples, boolean includeSamples) {
        List<QuerySamplingRecallResponse.Sample> contributions = includeSamples ? samples.stream().map(sample -> {
            OptionalDouble recall = RecallEstimator.recall(sample);
            return new QuerySamplingRecallResponse.Sample(
                sample.search().query().opaqueId(),
                sample.isEvent(),
                sample.weights().multiplicity(),
                sample.weights().weightedMultiplicity(),
                sample.weights().inclusionProbability(),
                sample.weights().seenProbability(),
                recall.isPresent() ? recall.getAsDouble() : null
            );
        }).toList() : null;
        return new QuerySamplingRecallResponse(RecallEstimator.estimate(samples), contributions);
    }
}
