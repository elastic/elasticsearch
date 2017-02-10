/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

import java.util.Objects;

public class ScrollDataExtractorFactory implements DataExtractorFactory {

    private final Client client;
    private final DatafeedConfig datafeedConfig;
    private final Job job;
    private final ExtractedFields extractedFields;

    public ScrollDataExtractorFactory(Client client, DatafeedConfig datafeedConfig, Job job) {
        this.client = Objects.requireNonNull(client);
        this.datafeedConfig = Objects.requireNonNull(datafeedConfig);
        this.job = Objects.requireNonNull(job);
        this.extractedFields = ExtractedFields.build(job, datafeedConfig);
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        ScrollDataExtractorContext dataExtractorContext = new ScrollDataExtractorContext(
                job.getId(),
                extractedFields,
                datafeedConfig.getIndexes(),
                datafeedConfig.getTypes(),
                datafeedConfig.getQuery(),
                datafeedConfig.getScriptFields(),
                datafeedConfig.getScrollSize(),
                start,
                end);
        return new ScrollDataExtractor(client, dataExtractorContext);
    }
}
