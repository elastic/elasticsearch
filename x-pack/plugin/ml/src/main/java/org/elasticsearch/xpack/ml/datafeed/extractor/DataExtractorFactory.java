/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregatedSearchRequestBuilder;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.CompositeAggregationDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.RollupDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.scroll.ScrollDataExtractorFactory;

public interface DataExtractorFactory {
    DataExtractor newExtractor(long start, long end);

    /**
     * Creates a {@code DataExtractorFactory} for the given datafeed-job combination.
     */
    static void create(
        Client client,
        DatafeedConfig datafeed,
        Job job,
        NamedXContentRegistry xContentRegistry,
        DatafeedTimingStatsReporter timingStatsReporter,
        ActionListener<DataExtractorFactory> listener
    ) {
        final boolean hasAggs = datafeed.hasAggregations();
        final boolean isComposite = hasAggs && datafeed.hasCompositeAgg(xContentRegistry);
        ActionListener<DataExtractorFactory> factoryHandler = ActionListener.wrap(
            factory -> listener.onResponse(
                datafeed.getChunkingConfig().isEnabled()
                    ? new ChunkedDataExtractorFactory(client, datafeed, job, xContentRegistry, factory, timingStatsReporter)
                    : factory
            ),
            listener::onFailure
        );

        ActionListener<GetRollupIndexCapsAction.Response> getRollupIndexCapsActionHandler = ActionListener.wrap(response -> {
            final boolean hasRollup = response.getJobs().isEmpty() == false;
            if (hasRollup && hasAggs == false) {
                listener.onFailure(new IllegalArgumentException("Aggregations are required when using Rollup indices"));
                return;
            }
            if (hasAggs == false) {
                ScrollDataExtractorFactory.create(client, datafeed, job, xContentRegistry, timingStatsReporter, factoryHandler);
                return;
            }
            if (hasRollup && datafeed.getRuntimeMappings().isEmpty() == false) {
                // TODO Rollup V2 will support runtime fields
                listener.onFailure(
                    new IllegalArgumentException(
                        "The datafeed has runtime_mappings defined, " + "runtime fields are not supported in rollup searches"
                    )
                );
                return;
            }
            if (isComposite) {
                String[] indices = datafeed.getIndices().toArray(new String[0]);
                IndicesOptions indicesOptions = datafeed.getIndicesOptions();
                AggregatedSearchRequestBuilder aggregatedSearchRequestBuilder = hasRollup
                    ? RollupDataExtractorFactory.requestBuilder(client, indices, indicesOptions)
                    : AggregationDataExtractorFactory.requestBuilder(client, indices, indicesOptions);
                final DataExtractorFactory dataExtractorFactory = new CompositeAggregationDataExtractorFactory(
                    client,
                    datafeed,
                    job,
                    xContentRegistry,
                    timingStatsReporter,
                    aggregatedSearchRequestBuilder
                );
                if (datafeed.getChunkingConfig().isManual()) {
                    factoryHandler.onResponse(dataExtractorFactory);
                } else {
                    listener.onResponse(dataExtractorFactory);
                }
                return;
            }

            if (hasRollup) {
                RollupDataExtractorFactory.create(
                    client,
                    datafeed,
                    job,
                    response.getJobs(),
                    xContentRegistry,
                    timingStatsReporter,
                    factoryHandler
                );
            } else {
                factoryHandler.onResponse(
                    new AggregationDataExtractorFactory(client, datafeed, job, xContentRegistry, timingStatsReporter)
                );
            }
        }, e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof IndexNotFoundException notFound) {
                listener.onFailure(
                    new ResourceNotFoundException(
                        "datafeed [" + datafeed.getId() + "] cannot retrieve data because index " + notFound.getIndex() + " does not exist"
                    )
                );
            } else {
                listener.onFailure(e);
            }
        });

        if (RemoteClusterLicenseChecker.containsRemoteIndex(datafeed.getIndices())) {
            // If we have remote indices in the data feed, don't bother checking for rollup support
            // Rollups + CCS is not supported
            getRollupIndexCapsActionHandler.onResponse(new GetRollupIndexCapsAction.Response());
        } else {
            ClientHelper.executeAsyncWithOrigin(
                client,
                ClientHelper.ML_ORIGIN,
                GetRollupIndexCapsAction.INSTANCE,
                new GetRollupIndexCapsAction.Request(datafeed.getIndices().toArray(new String[0]), datafeed.getIndicesOptions()),
                getRollupIndexCapsActionHandler
            );
        }
    }
}
