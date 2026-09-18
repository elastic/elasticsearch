/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.Collections;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;

public class DatafeedJobValidatorTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testEsqlGroupingIntervalDifferentFromBucketSpanShouldReject() {
        Job job = new Job.Builder("esql-job").setAnalysisConfig(JobTests.createAnalysisConfig().setBucketSpan(TimeValue.timeValueHours(1)))
            .setDataDescription(new DataDescription.Builder())
            .build(new Date());

        DatafeedConfig datafeed = new DatafeedConfig.Builder("esql-datafeed", "esql-job").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueMinutes(30))
            .setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig())
            .build();

        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeed, job, xContentRegistry())
        );
        assertThat(
            exception.getMessage(),
            equalTo(Messages.getMessage(Messages.DATAFEED_ESQL_GROUPING_INTERVAL_MUST_MATCH_BUCKET_SPAN, "30m", "1h"))
        );
    }
}
