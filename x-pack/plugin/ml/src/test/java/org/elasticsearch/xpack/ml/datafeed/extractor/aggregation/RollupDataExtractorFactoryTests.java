/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RollupDataExtractorFactoryTests extends ESTestCase {

    public void testCreateWithRuntimeFields() {
        String jobId = "foojob";

        Detector.Builder detectorBuilder = new Detector.Builder();
        detectorBuilder.setFunction("sum");
        detectorBuilder.setFieldName("value");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detectorBuilder.build()));
        Job.Builder jobBuilder = new Job.Builder(jobId);
        jobBuilder.setDataDescription(new DataDescription.Builder());
        jobBuilder.setAnalysisConfig(analysisConfig);

        DatafeedConfig.Builder datafeedConfigBuilder = new DatafeedConfig.Builder("foo-feed", jobId);
        datafeedConfigBuilder.setIndices(Collections.singletonList("my_index"));

        Map<String, Object> settings = new HashMap<>();
        settings.put("type", "keyword");
        settings.put("script", "");
        Map<String, Object> field = new HashMap<>();
        field.put("runtime_field_bar", settings);
        datafeedConfigBuilder.setRuntimeMappings(field);

        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        ActionListener<DataExtractorFactory> listener = ActionListener.wrap(
            r -> fail("unexpected response"),
            exceptionRef::set
        );

        RollupDataExtractorFactory.create(mock(Client.class), datafeedConfigBuilder.build(), jobBuilder.build(new Date()),
            Collections.emptyMap(), xContentRegistry(), mock(DatafeedTimingStatsReporter.class), listener);

        assertNotNull(exceptionRef.get());
        Exception e = exceptionRef.get();
        assertThat(e, instanceOf(IllegalArgumentException.class));
        assertThat(e.getMessage(), equalTo("The datafeed has runtime_mappings defined, " +
            "runtime fields are not supported in rollup searches"));
    }
}
