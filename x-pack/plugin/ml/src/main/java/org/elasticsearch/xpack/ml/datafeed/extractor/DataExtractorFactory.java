/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.rollup.RollupDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.scroll.ScrollDataExtractorFactory;
import org.elasticsearch.xpack.ml.notifications.Auditor;

public interface DataExtractorFactory {
    DataExtractor newExtractor(long start, long end);

    /**
     * Creates a {@code DataExtractorFactory} for the given datafeed-job combination.
     */
    static void create(Client client, DatafeedConfig datafeed, Job job, Auditor auditor, ActionListener<DataExtractorFactory> listener) {
        ActionListener<DataExtractorFactory> factoryHandler = ActionListener.wrap(
            factory -> {
                if (datafeed.getChunkingConfig().isEnabled()) {
                    auditor.info(job.getId(), "Creating chunked data extractor for datafeed [" + datafeed.getId() + "]");
                    listener.onResponse(new ChunkedDataExtractorFactory(client, datafeed, job, factory));
                } else {
                    listener.onResponse(factory);
                }
            }
            , listener::onFailure
        );

        ActionListener<GetRollupIndexCapsAction.Response> getRollupIndexCapsActionHandler = ActionListener.wrap(
            response -> {
                if (response.getJobs().isEmpty()) { // This means no rollup indexes are in the config
                    if (datafeed.hasAggregations()) {
                        auditor.info(job.getId(), "Creating aggregated data extractor for datafeed [" + datafeed.getId() + "]");
                        factoryHandler.onResponse(new AggregationDataExtractorFactory(client, datafeed, job));
                    } else {
                        auditor.info(job.getId(), "Creating scrolling data extractor for datafeed [" + datafeed.getId() + "]");
                        ScrollDataExtractorFactory.create(client, datafeed, job, factoryHandler);
                    }
                } else {
                    if (datafeed.hasAggregations()) { // Rollup indexes require aggregations
                        auditor.info(job.getId(), "Creating rollup data extractor for datafeed [" + datafeed.getId() + "]");
                        RollupDataExtractorFactory.create(client, datafeed, job, response.getJobs(), factoryHandler);
                    } else {
                        throw new IllegalArgumentException("Aggregations are required when using Rollup indices");
                    }
                }
            },
            listener::onFailure
        );

        ClientHelper.<GetRollupIndexCapsAction.Response> executeWithHeaders(datafeed.getHeaders(), ClientHelper.ML_ORIGIN, client, () -> {
            client.execute(GetRollupIndexCapsAction.INSTANCE,
                new GetRollupIndexCapsAction.Request(datafeed.getIndices().toArray(new String[datafeed.getIndices().size()])),
                    getRollupIndexCapsActionHandler);
            // This response gets discarded - the listener handles the real response
            return null;
        });
    }
}
