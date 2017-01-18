/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler.extractor.scroll;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.ml.scheduler.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.scheduler.extractor.DataExtractorFactory;

import java.util.Objects;

public class ScrollDataExtractorFactory implements DataExtractorFactory {

    private final Client client;
    private final SchedulerConfig schedulerConfig;
    private final Job job;
    private final ExtractedFields extractedFields;

    public ScrollDataExtractorFactory(Client client, SchedulerConfig schedulerConfig, Job job) {
        this.client = Objects.requireNonNull(client);
        this.schedulerConfig = Objects.requireNonNull(schedulerConfig);
        this.job = Objects.requireNonNull(job);
        this.extractedFields = ExtractedFields.build(job, schedulerConfig);
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        ScrollDataExtractorContext dataExtractorContext = new ScrollDataExtractorContext(
                job.getId(),
                extractedFields,
                schedulerConfig.getIndexes(),
                schedulerConfig.getTypes(),
                schedulerConfig.getQuery(),
                schedulerConfig.getScriptFields(),
                schedulerConfig.getScrollSize(),
                start,
                end);
        return new ScrollDataExtractor(client, dataExtractorContext);
    }
}
