/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;

import org.elasticsearch.client.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class JobTests extends AbstractXContentTestCase<Job> {

    private static final String FUTURE_JOB = "{\n" +
        "    \"job_id\": \"farequote\",\n" +
        "    \"create_time\": 1234567890000,\n" +
        "    \"tomorrows_technology_today\": \"wow\",\n" +
        "    \"analysis_config\": {\n" +
        "        \"bucket_span\": \"1h\",\n" +
        "        \"something_new\": \"gasp\",\n" +
        "        \"detectors\": [{\"function\": \"metric\", \"field_name\": \"responsetime\", \"by_field_name\": \"airline\"}]\n" +
        "    },\n" +
        "    \"data_description\": {\n" +
        "        \"time_field\": \"time\",\n" +
        "        \"the_future\": 123\n" +
        "    }\n" +
        "}";

    @Override
    protected Job createTestInstance() {
        return createRandomizedJob();
    }

    @Override
    protected Job doParseInstance(XContentParser parser) {
        return Job.PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testFutureMetadataParse() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, FUTURE_JOB);
        // The parser should tolerate unknown fields
        assertNotNull(Job.PARSER.apply(parser, null).build());
    }

    public void testCopyConstructor() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            Job job = createTestInstance();
            Job copy = new Job.Builder(job).build();
            assertEquals(job, copy);
        }
    }

    public void testBuilder_WithNullID() {
        Job.Builder builder = new Job.Builder("anything").setId(null);
        NullPointerException ex = expectThrows(NullPointerException.class, builder::build);
        assertEquals("[job_id] must not be null", ex.getMessage());
    }

    public void testBuilder_WithNullJobType() {
        Job.Builder builder = new Job.Builder("anything").setJobType(null);
        NullPointerException ex = expectThrows(NullPointerException.class, builder::build);
        assertEquals("[job_type] must not be null", ex.getMessage());
    }

    public static String randomValidJobId() {
        CodepointSetGenerator generator = new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    public static AnalysisConfig.Builder createAnalysisConfig() {
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("client");
        Detector.Builder d2 = new Detector.Builder("min", "field");
        return new AnalysisConfig.Builder(Arrays.asList(d1.build(), d2.build()));
    }

    public static Job.Builder createRandomizedJobBuilder() {
        String jobId = randomValidJobId();
        Job.Builder builder = new Job.Builder(jobId);
        if (randomBoolean()) {
            builder.setDescription(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            int groupsNum = randomIntBetween(0, 10);
            List<String> groups = new ArrayList<>(groupsNum);
            for (int i = 0; i < groupsNum; i++) {
                groups.add(randomValidJobId());
            }
            builder.setGroups(groups);
        }
        builder.setCreateTime(new Date(randomNonNegativeLong()));
        if (randomBoolean()) {
            builder.setFinishedTime(new Date(randomNonNegativeLong()));
        }
        builder.setAnalysisConfig(AnalysisConfigTests.createRandomized());
        builder.setAnalysisLimits(AnalysisLimitsTests.createRandomized());

        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(randomFrom(DataDescription.DataFormat.values()));
        builder.setDataDescription(dataDescription);

        if (randomBoolean()) {
            builder.setModelPlotConfig(ModelPlotConfigTests.createRandomized());
        }
        if (randomBoolean()) {
            builder.setRenormalizationWindowDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setBackgroundPersistInterval(TimeValue.timeValueHours(randomIntBetween(1, 24)));
        }
        Long modelSnapshotRetentionDays = null;
        if (randomBoolean()) {
            modelSnapshotRetentionDays = randomNonNegativeLong();
            builder.setModelSnapshotRetentionDays(modelSnapshotRetentionDays);
        }
        if (randomBoolean()) {
            if (modelSnapshotRetentionDays != null) {
                builder.setDailyModelSnapshotRetentionAfterDays(randomLongBetween(0, modelSnapshotRetentionDays));
            } else {
                builder.setDailyModelSnapshotRetentionAfterDays(randomNonNegativeLong());
            }
        }
        if (randomBoolean()) {
            builder.setResultsRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setSystemAnnotationsRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setCustomSettings(Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
        if (randomBoolean()) {
            builder.setModelSnapshotId(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            builder.setResultsIndexName(randomValidJobId());
        }
        if (randomBoolean()) {
            builder.setDeleting(randomBoolean());
        }
        if (randomBoolean()) {
            builder.setAllowLazyOpen(randomBoolean());
        }
        if (randomBoolean()) {
            builder.setBlocked(BlockedTests.createRandom());
        }
        if (randomBoolean()) {
            builder.setDatafeed(DatafeedConfigTests.createRandomBuilder(jobId, jobId));
        }
        return builder;
    }

    public static Job createRandomizedJob() {
        return createRandomizedJobBuilder().build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }
}
