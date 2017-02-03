/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.Job;

import java.util.Objects;

public class ChunkedDataExtractorFactory implements DataExtractorFactory {

    private final Client client;
    private final DatafeedConfig datafeedConfig;
    private final Job job;
    private final DataExtractorFactory dataExtractorFactory;

    public ChunkedDataExtractorFactory(Client client, DatafeedConfig datafeedConfig, Job job, DataExtractorFactory dataExtractorFactory) {
        this.client = Objects.requireNonNull(client);
        this.datafeedConfig = Objects.requireNonNull(datafeedConfig);
        this.job = Objects.requireNonNull(job);
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        ChunkedDataExtractorContext dataExtractorContext = new ChunkedDataExtractorContext(
                job.getId(),
                job.getDataDescription().getTimeField(),
                datafeedConfig.getIndexes(),
                datafeedConfig.getTypes(),
                datafeedConfig.getQuery(),
                datafeedConfig.getScrollSize(),
                start,
                end,
                datafeedConfig.getChunkingConfig() == null ? null : datafeedConfig.getChunkingConfig().getTimeSpan());
        return new ChunkedDataExtractor(client, dataExtractorFactory, dataExtractorContext);
    }
}
