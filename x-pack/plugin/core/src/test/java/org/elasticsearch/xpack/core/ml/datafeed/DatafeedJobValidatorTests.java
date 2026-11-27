/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.Collections;
import java.util.Date;

public class DatafeedJobValidatorTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    /** The time column's interval is known only from {@code DESCRIBE}, so the ML plugin checks the frequency against it. */
    public void testEsqlFrequencyNotMultipleOfBucketSpanShouldAccept() {
        Job job = new Job.Builder("esql-job").setAnalysisConfig(JobTests.createAnalysisConfig().setBucketSpan(TimeValue.timeValueHours(1)))
            .setDataDescription(new DataDescription.Builder())
            .build(new Date());

        DatafeedConfig datafeed = new DatafeedConfig.Builder("esql-datafeed", "esql-job").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setFrequency(TimeValue.timeValueMinutes(10))
            .setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig())
            .build();

        DatafeedJobValidator.validate(datafeed, job, xContentRegistry());
    }
}
