/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.protocol.xpack.ml.job.config;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public void testEquals_GivenDifferentClass() {
        Job job = buildJobBuilder("foo").build();
        assertFalse(job.equals("a string"));
    }

    public void testEquals_GivenDifferentIds() {
        Date createTime = new Date();
        Job.Builder builder = buildJobBuilder("foo");
        builder.setCreateTime(createTime);
        Job job1 = builder.build();
        builder.setId("bar");
        Job job2 = builder.build();
        assertFalse(job1.equals(job2));
    }

    public void testEquals_GivenDifferentRenormalizationWindowDays() {
        Date date = new Date();
        Job.Builder jobDetails1 = new Job.Builder("foo");
        jobDetails1.setDataDescription(new DataDescription.Builder());
        jobDetails1.setAnalysisConfig(createAnalysisConfig());
        jobDetails1.setRenormalizationWindowDays(3L);
        jobDetails1.setCreateTime(date);
        Job.Builder jobDetails2 = new Job.Builder("foo");
        jobDetails2.setDataDescription(new DataDescription.Builder());
        jobDetails2.setRenormalizationWindowDays(4L);
        jobDetails2.setAnalysisConfig(createAnalysisConfig());
        jobDetails2.setCreateTime(date);
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
    }

    public void testEquals_GivenDifferentBackgroundPersistInterval() {
        Date date = new Date();
        Job.Builder jobDetails1 = new Job.Builder("foo");
        jobDetails1.setDataDescription(new DataDescription.Builder());
        jobDetails1.setAnalysisConfig(createAnalysisConfig());
        jobDetails1.setBackgroundPersistInterval(TimeValue.timeValueSeconds(10000L));
        jobDetails1.setCreateTime(date);
        Job.Builder jobDetails2 = new Job.Builder("foo");
        jobDetails2.setDataDescription(new DataDescription.Builder());
        jobDetails2.setBackgroundPersistInterval(TimeValue.timeValueSeconds(8000L));
        jobDetails2.setAnalysisConfig(createAnalysisConfig());
        jobDetails2.setCreateTime(date);
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
    }

    public void testEquals_GivenDifferentModelSnapshotRetentionDays() {
        Date date = new Date();
        Job.Builder jobDetails1 = new Job.Builder("foo");
        jobDetails1.setDataDescription(new DataDescription.Builder());
        jobDetails1.setAnalysisConfig(createAnalysisConfig());
        jobDetails1.setModelSnapshotRetentionDays(10L);
        jobDetails1.setCreateTime(date);
        Job.Builder jobDetails2 = new Job.Builder("foo");
        jobDetails2.setDataDescription(new DataDescription.Builder());
        jobDetails2.setModelSnapshotRetentionDays(8L);
        jobDetails2.setAnalysisConfig(createAnalysisConfig());
        jobDetails2.setCreateTime(date);
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
    }

    public void testEquals_GivenDifferentResultsRetentionDays() {
        Date date = new Date();
        Job.Builder jobDetails1 = new Job.Builder("foo");
        jobDetails1.setDataDescription(new DataDescription.Builder());
        jobDetails1.setAnalysisConfig(createAnalysisConfig());
        jobDetails1.setCreateTime(date);
        jobDetails1.setResultsRetentionDays(30L);
        Job.Builder jobDetails2 = new Job.Builder("foo");
        jobDetails2.setDataDescription(new DataDescription.Builder());
        jobDetails2.setResultsRetentionDays(4L);
        jobDetails2.setAnalysisConfig(createAnalysisConfig());
        jobDetails2.setCreateTime(date);
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
    }

    public void testEquals_GivenDifferentCustomSettings() {
        Job.Builder jobDetails1 = buildJobBuilder("foo");
        Map<String, Object> customSettings1 = new HashMap<>();
        customSettings1.put("key1", "value1");
        jobDetails1.setCustomSettings(customSettings1);
        Job.Builder jobDetails2 = buildJobBuilder("foo");
        Map<String, Object> customSettings2 = new HashMap<>();
        customSettings2.put("key2", "value2");
        jobDetails2.setCustomSettings(customSettings2);
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
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

    public static Job.Builder buildJobBuilder(String id, Date date) {
        Job.Builder builder = new Job.Builder(id);
        builder.setCreateTime(date);
        AnalysisConfig.Builder ac = createAnalysisConfig();
        DataDescription.Builder dc = new DataDescription.Builder();
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(dc);
        return builder;
    }

    public static Job.Builder buildJobBuilder(String id) {
        return buildJobBuilder(id, new Date());
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
        if (randomBoolean()) {
            builder.setLastDataTime(new Date(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            builder.setEstablishedModelMemory(randomNonNegativeLong());
        }
        builder.setAnalysisConfig(AnalysisConfigTests.createRandomized());
        builder.setAnalysisLimits(AnalysisLimitsTests.createRandomized());

        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(randomFrom(DataDescription.DataFormat.values()));
        builder.setDataDescription(dataDescription);

        if (randomBoolean()) {
            builder.setModelPlotConfig(new ModelPlotConfig(randomBoolean(), randomAlphaOfLength(10)));
        }
        if (randomBoolean()) {
            builder.setRenormalizationWindowDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setBackgroundPersistInterval(TimeValue.timeValueHours(randomIntBetween(1, 24)));
        }
        if (randomBoolean()) {
            builder.setModelSnapshotRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setResultsRetentionDays(randomNonNegativeLong());
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
