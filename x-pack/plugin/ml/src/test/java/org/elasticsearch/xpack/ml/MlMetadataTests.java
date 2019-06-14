/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedConfig;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class MlMetadataTests extends AbstractSerializingTestCase<MlMetadata> {

    @Override
    protected MlMetadata createTestInstance() {
        MlMetadata.Builder builder = new MlMetadata.Builder();
        int numJobs = randomIntBetween(0, 10);
        for (int i = 0; i < numJobs; i++) {
            Job job = JobTests.createRandomizedJob();
            if (randomBoolean()) {
                AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(job.getAnalysisConfig());
                analysisConfig.setLatency(null);
                DatafeedConfig datafeedConfig = DatafeedConfigTests.createRandomizedDatafeedConfig(
                        job.getId(), job.getAnalysisConfig().getBucketSpan().millis());
                if (datafeedConfig.hasAggregations()) {
                    analysisConfig.setSummaryCountFieldName("doc_count");
                }
                job = new Job.Builder(job).setAnalysisConfig(analysisConfig).build();
                builder.putJob(job, false);
                builder.putDatafeed(datafeedConfig, Collections.emptyMap(), xContentRegistry());
            } else {
                builder.putJob(job, false);
            }
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<MlMetadata> instanceReader() {
        return MlMetadata::new;
    }

    @Override
    protected MlMetadata doParseInstance(XContentParser parser) {
        return MlMetadata.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testPutJob() {
        Job job1 = buildJobBuilder("1").build();
        Job job2 = buildJobBuilder("2").build();

        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putJob(job2, false);

        MlMetadata result = builder.build();
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("1"), nullValue());
        assertThat(result.getJobs().get("2"), sameInstance(job2));
        assertThat(result.getDatafeeds().get("2"), nullValue());

        builder = new MlMetadata.Builder(result);

        MlMetadata.Builder builderReference = builder;
        ResourceAlreadyExistsException e = expectThrows(ResourceAlreadyExistsException.class, () -> builderReference.putJob(job2, false));
        assertEquals("The job cannot be created with the Id '2'. The Id is already used.", e.getMessage());
        Job job2Attempt2 = buildJobBuilder("2").build();
        builder.putJob(job2Attempt2, true);

        result = builder.build();
        assertThat(result.getJobs().size(), equalTo(2));
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getJobs().get("2"), sameInstance(job2Attempt2));
    }

    public void testExpandJobIds() {
        MlMetadata mlMetadata = newMlMetadataWithJobs("bar-1", "foo-1", "foo-2").build();

        assertThat(mlMetadata.expandJobIds("_all", false), contains("bar-1", "foo-1", "foo-2"));
        assertThat(mlMetadata.expandJobIds("*", false), contains("bar-1", "foo-1", "foo-2"));
        assertThat(mlMetadata.expandJobIds("foo-*", false), contains("foo-1", "foo-2"));
        assertThat(mlMetadata.expandJobIds("foo-1,bar-*", false), contains("bar-1", "foo-1"));
    }

    public void testExpandDatafeedIds() {
        MlMetadata.Builder mlMetadataBuilder = newMlMetadataWithJobs("bar-1", "foo-1", "foo-2");
        List<DatafeedConfig> datafeeds = new ArrayList<>();
        datafeeds.add(createDatafeedConfig("bar-1-feed", "bar-1").build());
        datafeeds.add(createDatafeedConfig("foo-1-feed", "foo-1").build());
        datafeeds.add(createDatafeedConfig("foo-2-feed", "foo-2").build());
        mlMetadataBuilder.putDatafeeds(datafeeds);
        MlMetadata mlMetadata = mlMetadataBuilder.build();

        assertThat(mlMetadata.expandDatafeedIds("_all", false), contains("bar-1-feed", "foo-1-feed", "foo-2-feed"));
        assertThat(mlMetadata.expandDatafeedIds("*", false), contains("bar-1-feed", "foo-1-feed", "foo-2-feed"));
        assertThat(mlMetadata.expandDatafeedIds("foo-*", false), contains("foo-1-feed", "foo-2-feed"));
        assertThat(mlMetadata.expandDatafeedIds("foo-1-feed,bar-1*", false), contains("bar-1-feed", "foo-1-feed"));
    }

    private static MlMetadata.Builder newMlMetadataWithJobs(String... jobIds) {
        MlMetadata.Builder builder = new MlMetadata.Builder();
        for (String jobId : jobIds) {
            Job job = buildJobBuilder(jobId).build();
            builder.putJob(job, false);
        }
        return builder;
    }

    @Override
    protected MlMetadata mutateInstance(MlMetadata instance) {
        Map<String, Job> jobs = instance.getJobs();
        Map<String, DatafeedConfig> datafeeds = instance.getDatafeeds();
        MlMetadata.Builder metadataBuilder = new MlMetadata.Builder();

        for (Map.Entry<String, Job> entry : jobs.entrySet()) {
            metadataBuilder.putJob(entry.getValue(), true);
        }
        for (Map.Entry<String, DatafeedConfig> entry : datafeeds.entrySet()) {
            metadataBuilder.putDatafeed(entry.getValue(), Collections.emptyMap(), xContentRegistry());
        }

        switch (between(0, 1)) {
        case 0:
            metadataBuilder.putJob(JobTests.createRandomizedJob(), true);
            break;
        case 1:
            // Because we check if the job for the datafeed exists and we don't
            // allow two datafeeds to exist for a single job we have to add both
            // a job and a datafeed here
            Job randomJob = JobTests.createRandomizedJob();
            AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(randomJob.getAnalysisConfig());
            analysisConfig.setLatency(null);
            DatafeedConfig datafeedConfig = DatafeedConfigTests.createRandomizedDatafeedConfig(randomJob.getId(),
                    randomJob.getAnalysisConfig().getBucketSpan().millis());
            if (datafeedConfig.hasAggregations()) {
                analysisConfig.setSummaryCountFieldName("doc_count");
            }
            randomJob = new Job.Builder(randomJob).setAnalysisConfig(analysisConfig).build();
            metadataBuilder.putJob(randomJob, false);
            metadataBuilder.putDatafeed(datafeedConfig, Collections.emptyMap(), xContentRegistry());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return metadataBuilder.build();
    }
}
