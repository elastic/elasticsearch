/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.scroll.ScrollDataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.Job;

public interface DataExtractorFactory {
    DataExtractor newExtractor(long start, long end);

    /**
     * Creates a {@code DataExtractorFactory} for the given datafeed-job combination.
     */
    static void create(Client client, DatafeedConfig datafeed, Job job, ActionListener<DataExtractorFactory> listener) {
        ActionListener<DataExtractorFactory> factoryHandler = ActionListener.wrap(
                factory -> listener.onResponse(datafeed.getChunkingConfig().isEnabled()
                        ? new ChunkedDataExtractorFactory(client, datafeed, job, factory) : factory)
                , listener::onFailure
        );

        boolean isScrollSearch = datafeed.hasAggregations() == false;
        if (isScrollSearch) {
            ScrollDataExtractorFactory.create(client, datafeed, job, factoryHandler);
        } else {
            factoryHandler.onResponse(new AggregationDataExtractorFactory(client, datafeed, job));
        }
    }
}
