/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.autoscaling.Autoscaling;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResults;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.ml.LocalStateMachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingDeciderService;
import org.elasticsearch.xpack.ml.autoscaling.MlScalingReason;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class AutoscalingIT extends MlNativeAutodetectIntegTestCase {

    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWritableEntries() {
        List<NamedWriteableRegistry.Entry> entries = super.getNamedWritableEntries();
        entries.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, AutoscalingMetadata.NAME, AutoscalingMetadata::new));
        entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class,
            AutoscalingMetadata.NAME,
            AutoscalingMetadata.AutoscalingMetadataDiff::new));
        entries.add(new NamedWriteableRegistry.Entry(
            AutoscalingDeciderResult.Reason.class,
            MlScalingReason.NAME,
            MlScalingReason::new
        ));
        return entries;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            LocalStateMachineLearning.class,
            Netty4Plugin.class,
            ReindexPlugin.class,
            Autoscaling.class,
            // The monitoring plugin requires script and gsub processors to be loaded
            IngestCommonPlugin.class,
            // The monitoring plugin script processor references painless. Include this for script compilation.
            // This is to reduce log spam
            MockPainlessScriptEngine.TestPlugin.class,
            // ILM is required for .ml-state template index settings
            IndexLifecycle.class,
            DataStreamsPlugin.class);
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder().put(super.externalClusterClientSettings())
            .put(Autoscaling.AUTOSCALING_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testMLAutoscalingCapacity() {
        SortedMap<String, Settings> deciders = new TreeMap<>();
        deciders.put(MlAutoscalingDeciderService.NAME, Settings.builder().build());
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            "ml_test",
            new TreeSet<>(Arrays.asList("ml")),
            deciders
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());

        assertMlCapacity(
            client().execute(
                GetAutoscalingCapacityAction.INSTANCE,
                new GetAutoscalingCapacityAction.Request()
            ).actionGet(),
            "Requesting scale down as tier and/or node size could be smaller",
            0L,
            0L);

        putJob("job1", 100);
        putJob("job2", 200);
        openJob("job1");
        openJob("job2");

        assertMlCapacity(
            client().execute(
                GetAutoscalingCapacityAction.INSTANCE,
                new GetAutoscalingCapacityAction.Request()
            ).actionGet(),
            "Requesting scale down as tier and/or node size could be smaller",
            1328196267L,
            734003200L);

        putJob("bigjob1", 20_000);
        putJob("bigjob2", 10_000);
        putJob("bigjob3", 30_000);
        openJob("bigjob1");
        openJob("bigjob2");
        openJob("bigjob3");

        assertMlCapacity(
            client().execute(
                GetAutoscalingCapacityAction.INSTANCE,
                new GetAutoscalingCapacityAction.Request()
            ).actionGet(),
            "requesting scale up as number of jobs in queues exceeded configured limit",
            380991001934L,
            104892552534L);

        closeJob("bigjob1");
        closeJob("bigjob2");
        closeJob("bigjob3");

        assertMlCapacity(
            client().execute(
                GetAutoscalingCapacityAction.INSTANCE,
                new GetAutoscalingCapacityAction.Request()
            ).actionGet(),
            "Requesting scale down as tier and/or node size could be smaller",
            1328196267L,
            734003200L);
        closeJob("job1");
        closeJob("job2");

        assertMlCapacity(
            client().execute(
                GetAutoscalingCapacityAction.INSTANCE,
                new GetAutoscalingCapacityAction.Request()
            ).actionGet(),
            "Requesting scale down as tier and/or node size could be smaller",
            0L,
            0L);
    }

    private void assertMlCapacity(GetAutoscalingCapacityAction.Response capacity, String reason, long tierBytes, long nodeBytes) {
        assertThat(capacity.getResults(), hasKey("ml_test"));
        AutoscalingDeciderResults autoscalingDeciderResults = capacity.getResults().get("ml_test");
        assertThat(autoscalingDeciderResults.results(), hasKey("ml"));

        AutoscalingDeciderResult autoscalingDeciderResult = autoscalingDeciderResults.results().get("ml");
        assertThat(autoscalingDeciderResult.reason().summary(), containsString(reason));
        assertThat(autoscalingDeciderResult.requiredCapacity().tier().memory().getBytes(), equalTo(tierBytes));
        assertThat(autoscalingDeciderResult.requiredCapacity().node().memory().getBytes(), equalTo(nodeBytes));
    }

    private void putJob(String jobId, long limitMb) {
        Job.Builder job =
            new Job.Builder(jobId)
                .setAllowLazyOpen(true)
                .setAnalysisLimits(new AnalysisLimits(limitMb, null))
                .setAnalysisConfig(
                    new AnalysisConfig.Builder((List<Detector>) null)
                        .setBucketSpan(TimeValue.timeValueHours(1))
                        .setDetectors(
                            Collections.singletonList(
                                new Detector.Builder("count", null)
                                    .setPartitionFieldName("user")
                                    .build())))
                .setDataDescription(
                    new DataDescription.Builder()
                        .setTimeFormat("epoch"));

        registerJob(job);
        putJob(job);
    }
}
