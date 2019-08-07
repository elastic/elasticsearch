/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.RollupDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.scroll.ScrollDataExtractorFactory;

public interface DataExtractorFactory {
    DataExtractor newExtractor(long start, long end);

    /**
     * Creates a {@code DataExtractorFactory} for the given datafeed-job combination.
     */
    static void create(Client client,
                       DatafeedConfig datafeed,
                       Job job,
                       NamedXContentRegistry xContentRegistry,
                       DatafeedTimingStatsReporter timingStatsReporter,
                       ActionListener<DataExtractorFactory> listener) {
        ActionListener<DataExtractorFactory> factoryHandler = ActionListener.wrap(
            factory -> listener.onResponse(datafeed.getChunkingConfig().isEnabled()
                ? new ChunkedDataExtractorFactory(client, datafeed, job, xContentRegistry, factory, timingStatsReporter) : factory)
            , listener::onFailure
        );

        ActionListener<GetRollupIndexCapsAction.Response> getRollupIndexCapsActionHandler = ActionListener.wrap(
            response -> {
                if (response.getJobs().isEmpty()) { // This means no rollup indexes are in the config
                    if (datafeed.hasAggregations()) {
                        factoryHandler.onResponse(
                            new AggregationDataExtractorFactory(client, datafeed, job, xContentRegistry, timingStatsReporter));
                    } else {
                        ScrollDataExtractorFactory.create(client, datafeed, job, xContentRegistry, timingStatsReporter, factoryHandler);
                    }
                } else {
                    if (datafeed.hasAggregations()) { // Rollup indexes require aggregations
                        RollupDataExtractorFactory.create(
                            client, datafeed, job, response.getJobs(), xContentRegistry, timingStatsReporter, factoryHandler);
                    } else {
                        listener.onFailure(new IllegalArgumentException("Aggregations are required when using Rollup indices"));
                    }
                }
            },
            e -> {
                if (e instanceof IndexNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException("datafeed [" + datafeed.getId()
                        + "] cannot retrieve data because index " + ((IndexNotFoundException)e).getIndex() + " does not exist"));
                } else {
                    listener.onFailure(e);
                }
            }
        );

        if (RemoteClusterLicenseChecker.containsRemoteIndex(datafeed.getIndices())) {
            // If we have remote indices in the data feed, don't bother checking for rollup support
            // Rollups + CCS is not supported
            getRollupIndexCapsActionHandler.onResponse(new GetRollupIndexCapsAction.Response());
        } else {
            ClientHelper.executeAsyncWithOrigin(
                client,
                ClientHelper.ML_ORIGIN,
                GetRollupIndexCapsAction.INSTANCE,
                new GetRollupIndexCapsAction.Request(datafeed.getIndices().toArray(new String[0])),
                getRollupIndexCapsActionHandler);
        }
    }
}
