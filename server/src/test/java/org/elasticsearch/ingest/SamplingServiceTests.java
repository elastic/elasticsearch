/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.admin.indices.sampling.SamplingConfiguration;
import org.elasticsearch.action.admin.indices.sampling.SamplingMetadata;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class SamplingServiceTests extends ESTestCase {

    private static final String TEST_CONDITIONAL_SCRIPT = "ctx?.foo == 'bar'";

    public void testMaybeSample() {
        SamplingService samplingService = getTestSamplingService();

        // First, test with a project that has no sampling config:
        String indexName = randomIdentifier();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(ProjectId.DEFAULT);
        final ProjectId projectId = projectBuilder.getId();
        ProjectMetadata projectMetadata = projectBuilder.build();
        Map<String, Object> inputRawDocSource = randomMap(1, 100, () -> Tuple.tuple(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        final IndexRequest indexRequest = new IndexRequest(indexName).id("_id").source(inputRawDocSource);
        samplingService.maybeSample(projectMetadata, indexRequest);
        assertThat(samplingService.getLocalSample(projectId, indexName), empty());

        // Now test with a valid configuration:
        int maxSize = 100;
        projectBuilder = ProjectMetadata.builder(projectMetadata)
            .putCustom(
                SamplingMetadata.TYPE,
                new SamplingMetadata(
                    Map.of(indexName, new SamplingConfiguration(1.0, maxSize, ByteSizeValue.ofMb(100), TimeValue.timeValueDays(3), null))
                )
            );
        projectMetadata = projectBuilder.build();
        int docsToSample = randomIntBetween(1, maxSize);
        for (int i = 0; i < docsToSample; i++) {
            samplingService.maybeSample(projectMetadata, indexRequest);
        }
        List<SamplingService.RawDocument> sample = samplingService.getLocalSample(projectId, indexName);
        assertThat(sample, not(empty()));
        // Since our sampling rate was 100%, we expect every document to have been sampled:
        assertThat(sample.size(), equalTo(docsToSample));
        SamplingService.RawDocument rawDocument = sample.getFirst();
        assertThat(rawDocument.indexName(), equalTo(indexName));
        Map<String, Object> outputRawDocSource = XContentHelper.convertToMap(
            rawDocument.contentType().xContent(),
            rawDocument.source(),
            0,
            rawDocument.source().length,
            randomBoolean()
        );
        assertThat(outputRawDocSource, equalTo(inputRawDocSource));

        SamplingService.SampleStats stats = samplingService.getLocalSampleStats(projectId, indexName);
        assertThat(stats.getSamples(), equalTo((long) docsToSample));
        assertThat(stats.getPotentialSamples(), equalTo((long) docsToSample));
        assertThat(stats.getSamplesRejectedForRate(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForCondition(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForException(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForMaxSamplesExceeded(), equalTo(0L));
        assertThat(stats.getLastException(), nullValue());
        assertThat(stats.getTimeSampling(), greaterThan(TimeValue.ZERO));
        assertThat(stats.getTimeCompilingCondition(), equalTo(TimeValue.ZERO));
        assertThat(stats.getTimeEvaluatingCondition(), equalTo(TimeValue.ZERO));
    }

    public void testMaybeSampleWithCondition() {
        SamplingService samplingService = getTestSamplingService();
        String indexName = randomIdentifier();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(
                SamplingMetadata.TYPE,
                new SamplingMetadata(
                    Map.of(
                        indexName,
                        new SamplingConfiguration(1.0, 100, ByteSizeValue.ofMb(100), TimeValue.timeValueDays(3), TEST_CONDITIONAL_SCRIPT)
                    )
                )
            );
        final ProjectId projectId = projectBuilder.getId();
        ProjectMetadata projectMetadata = projectBuilder.build();
        Map<String, Object> indexRequest1Source = Map.of("foo", "bar", "baz", "bop");
        final IndexRequest indexRequest1 = new IndexRequest(indexName).id("_id").source(indexRequest1Source);
        samplingService.maybeSample(projectMetadata, indexRequest1);
        final IndexRequest indexRequest2 = new IndexRequest(indexName).id("_id").source(Map.of("bar", "foo", "baz", "bop"));
        samplingService.maybeSample(projectMetadata, indexRequest2);
        List<SamplingService.RawDocument> sample = samplingService.getLocalSample(projectId, indexName);
        assertThat(sample.size(), equalTo(1));
        SamplingService.RawDocument rawDocument = sample.getFirst();
        assertThat(rawDocument.indexName(), equalTo(indexName));
        Map<String, Object> outputRawDocSource = XContentHelper.convertToMap(
            rawDocument.contentType().xContent(),
            rawDocument.source(),
            0,
            rawDocument.source().length,
            randomBoolean()
        );
        assertThat(outputRawDocSource, equalTo(indexRequest1Source));

        SamplingService.SampleStats stats = samplingService.getLocalSampleStats(projectId, indexName);
        assertThat(stats.getSamples(), equalTo((long) 1));
        assertThat(stats.getPotentialSamples(), equalTo((long) 2));
        assertThat(stats.getSamplesRejectedForRate(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForCondition(), equalTo(1L));
        assertThat(stats.getSamplesRejectedForException(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForMaxSamplesExceeded(), equalTo(0L));
        assertThat(stats.getLastException(), nullValue());
        assertThat(stats.getTimeSampling(), greaterThan(TimeValue.ZERO));
        assertThat(stats.getTimeCompilingCondition(), greaterThan(TimeValue.ZERO));
        assertThat(stats.getTimeEvaluatingCondition(), greaterThan(TimeValue.ZERO));
    }

    public void testMaybeSampleWithLowRate() {
        SamplingService samplingService = getTestSamplingService();
        String indexName = randomIdentifier();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(
                SamplingMetadata.TYPE,
                new SamplingMetadata(
                    Map.of(indexName, new SamplingConfiguration(0.001, 100, ByteSizeValue.ofMb(100), TimeValue.timeValueDays(3), null))
                )
            );
        final ProjectId projectId = projectBuilder.getId();
        ProjectMetadata projectMetadata = projectBuilder.build();
        Map<String, Object> inputRawDocSource = randomMap(1, 100, () -> Tuple.tuple(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        final IndexRequest indexRequest = new IndexRequest(indexName).id("_id").source(inputRawDocSource);
        for (int i = 0; i < 100; i++) {
            samplingService.maybeSample(projectMetadata, indexRequest);
        }
        /*
         * We had 100 chances to take a sample. We're sampling at a rate of one in 1000. The odds of even one are fairly low. The odds of
         * 10 are so low that we will almost certainly never see that unless there is a bug. We're really just making sure that we don't
         * introduce a bug where we ignore the rate.
         */
        int samples = samplingService.getLocalSample(projectId, indexName).size();
        assertThat(samples, lessThan(10));

        SamplingService.SampleStats stats = samplingService.getLocalSampleStats(projectId, indexName);
        assertThat(stats.getSamples(), equalTo((long) samples));
        assertThat(stats.getPotentialSamples(), equalTo(100L));
        assertThat(stats.getSamplesRejectedForRate(), equalTo((long) 100 - samples));
        assertThat(stats.getSamplesRejectedForCondition(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForException(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForMaxSamplesExceeded(), equalTo(0L));
        assertThat(stats.getLastException(), nullValue());
        assertThat(stats.getTimeSampling(), greaterThan(TimeValue.ZERO));
        assertThat(stats.getTimeCompilingCondition(), equalTo(TimeValue.ZERO));
        assertThat(stats.getTimeEvaluatingCondition(), equalTo(TimeValue.ZERO));
    }

    public void testMaybeSampleMaxSamples() {
        SamplingService samplingService = getTestSamplingService();
        String indexName = randomIdentifier();
        int maxSamples = randomIntBetween(1, 1000);
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(
                SamplingMetadata.TYPE,
                new SamplingMetadata(
                    Map.of(indexName, new SamplingConfiguration(1.0, maxSamples, ByteSizeValue.ofMb(100), TimeValue.timeValueDays(3), null))
                )
            );
        final ProjectId projectId = projectBuilder.getId();
        ProjectMetadata projectMetadata = projectBuilder.build();
        Map<String, Object> inputRawDocSource = randomMap(1, 100, () -> Tuple.tuple(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        final IndexRequest indexRequest = new IndexRequest(indexName).id("_id").source(inputRawDocSource);
        int docsToSample = randomIntBetween(maxSamples + 1, maxSamples + 1000);
        for (int i = 0; i < docsToSample; i++) {
            samplingService.maybeSample(projectMetadata, indexRequest);
        }
        assertThat(samplingService.getLocalSample(projectId, indexName).size(), equalTo(maxSamples));

        SamplingService.SampleStats stats = samplingService.getLocalSampleStats(projectId, indexName);
        assertThat(stats.getSamples(), equalTo((long) maxSamples));
        assertThat(stats.getPotentialSamples(), equalTo((long) docsToSample));
        assertThat(stats.getSamplesRejectedForRate(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForCondition(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForException(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForMaxSamplesExceeded(), equalTo((long) docsToSample - maxSamples));
        assertThat(stats.getLastException(), nullValue());
        assertThat(stats.getTimeSampling(), greaterThan(TimeValue.ZERO));
        assertThat(stats.getTimeCompilingCondition(), equalTo(TimeValue.ZERO));
        assertThat(stats.getTimeEvaluatingCondition(), equalTo(TimeValue.ZERO));
    }

    private SamplingService getTestSamplingService() {
        final ScriptService scriptService = new ScriptService(
            Settings.EMPTY,
            Map.of(Script.DEFAULT_SCRIPT_LANG, new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Map.of(TEST_CONDITIONAL_SCRIPT, ctx -> {
                Object fooVal = ctx.get("foo");
                return fooVal != null && fooVal.equals("bar");
            }), Map.of())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS),
            () -> 1L,
            TestProjectResolvers.singleProject(randomProjectIdOrDefault())
        );
        ClusterService clusterService = ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool());
        final ProjectId projectId = ProjectId.DEFAULT;
        final ProjectResolver projectResolver = TestProjectResolvers.singleProject(projectId);
        return new SamplingService(scriptService, clusterService, projectResolver, System::currentTimeMillis);
    }
}
