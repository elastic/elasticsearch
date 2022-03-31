/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResults;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingDeciderService;
import org.elasticsearch.xpack.ml.autoscaling.NativeMemoryCapacity;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.ml.integration.PyTorchModelIT.BASE_64_ENCODED_MODEL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;

public class AutoscalingIT extends MlNativeAutodetectIntegTestCase {

    private static final long BASIC_REQUIREMENT_MB = 10;
    private static final long NATIVE_PROCESS_OVERHEAD_MB = 30;
    private static final long BASELINE_OVERHEAD_MB = BASIC_REQUIREMENT_MB + NATIVE_PROCESS_OVERHEAD_MB;

    @Before
    public void putSettings() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(MachineLearning.MAX_LAZY_ML_NODES.getKey(), 100).put("logger.org.elasticsearch.xpack.ml", "TRACE")
            )
            .get();
    }

    @After
    public void removeSettings() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().putNull(MachineLearning.MAX_LAZY_ML_NODES.getKey()).putNull("logger.org.elasticsearch.xpack.ml")
            )
            .get();
        cleanUp();
    }

    // This test assumes that xpack.ml.max_machine_memory_percent is 30
    // and that xpack.ml.use_auto_machine_memory_percent is false
    public void testMLAutoscalingCapacity() throws Exception {
        SortedMap<String, Settings> deciders = new TreeMap<>();
        deciders.put(
            MlAutoscalingDeciderService.NAME,
            Settings.builder().put(MlAutoscalingDeciderService.DOWN_SCALE_DELAY.getKey(), TimeValue.ZERO).build()
        );
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            "ml_test",
            new TreeSet<>(Arrays.asList("master", "data", "ingest", "ml")),
            deciders
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());

        assertBusy(
            () -> assertMlCapacity(
                client().execute(GetAutoscalingCapacityAction.INSTANCE, new GetAutoscalingCapacityAction.Request()).actionGet(),
                "Requesting scale down as tier and/or node size could be smaller",
                0L,
                0L
            )
        );

        putJob("job1", 100);
        putJob("job2", 200);
        openJob("job1");
        openJob("job2");
        long expectedTierBytes = (long) Math.ceil(
            ByteSizeValue.ofMb(100 + BASELINE_OVERHEAD_MB + 200 + BASELINE_OVERHEAD_MB).getBytes() * 100 / 30.0
        );
        long expectedNodeBytes = (long) Math.ceil(ByteSizeValue.ofMb(200 + BASELINE_OVERHEAD_MB).getBytes() * 100 / 30.0);

        assertMlCapacity(
            client().execute(GetAutoscalingCapacityAction.INSTANCE, new GetAutoscalingCapacityAction.Request()).actionGet(),
            "Requesting scale down as tier and/or node size could be smaller",
            expectedTierBytes,
            expectedNodeBytes
        );

        putJob("bigjob1", 60_000);
        putJob("bigjob2", 50_000);
        openJob("bigjob1");
        openJob("bigjob2");
        List<DiscoveryNode> mlNodes = admin().cluster()
            .prepareNodesInfo()
            .all()
            .get()
            .getNodes()
            .stream()
            .map(NodeInfo::getNode)
            .filter(MachineLearning::isMlNode)
            .collect(Collectors.toList());
        NativeMemoryCapacity currentScale = MlAutoscalingDeciderService.currentScale(mlNodes, 30, false);
        expectedTierBytes = (long) Math.ceil(
            (ByteSizeValue.ofMb(50_000 + BASIC_REQUIREMENT_MB + 60_000 + BASELINE_OVERHEAD_MB).getBytes() + currentScale
                .getTierMlNativeMemoryRequirement()) * 100 / 30.0
        );
        expectedNodeBytes = (long) (ByteSizeValue.ofMb(60_000 + BASELINE_OVERHEAD_MB).getBytes() * 100 / 30.0);

        assertMlCapacity(
            client().execute(GetAutoscalingCapacityAction.INSTANCE, new GetAutoscalingCapacityAction.Request()).actionGet(),
            "requesting scale up as number of jobs in queues exceeded configured limit",
            expectedTierBytes,
            expectedNodeBytes
        );

        expectedTierBytes = (long) Math.ceil(
            ByteSizeValue.ofMb(100 + BASELINE_OVERHEAD_MB + 200 + BASELINE_OVERHEAD_MB).getBytes() * 100 / 30.0
        );
        expectedNodeBytes = (long) Math.ceil(ByteSizeValue.ofMb(200 + BASELINE_OVERHEAD_MB).getBytes() * 100 / 30.0);
        closeJob("bigjob1");
        closeJob("bigjob2");

        assertMlCapacity(
            client().execute(GetAutoscalingCapacityAction.INSTANCE, new GetAutoscalingCapacityAction.Request()).actionGet(),
            "Requesting scale down as tier and/or node size could be smaller",
            expectedTierBytes,
            expectedNodeBytes
        );
        closeJob("job1");
        closeJob("job2");

        assertMlCapacity(
            client().execute(GetAutoscalingCapacityAction.INSTANCE, new GetAutoscalingCapacityAction.Request()).actionGet(),
            "Requesting scale down as tier and/or node size could be smaller",
            0L,
            0L
        );
    }

    @AwaitsFix(bugUrl = "Cannot be fixed until we move estimation to config and not rely on definition length only")
    public void testMLAutoscalingForLargeModelAllocation() {
        String modelId = "really_big_model";
        SortedMap<String, Settings> deciders = new TreeMap<>();
        deciders.put(
            MlAutoscalingDeciderService.NAME,
            Settings.builder().put(MlAutoscalingDeciderService.DOWN_SCALE_DELAY.getKey(), TimeValue.ZERO).build()
        );
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            "ml_test",
            new TreeSet<>(Arrays.asList("master", "data", "ingest", "ml")),
            deciders
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
        putAndStartModelDeployment("smaller1", ByteSizeValue.ofMb(100).getBytes(), AllocationStatus.State.STARTED);
        putAndStartModelDeployment("smaller2", ByteSizeValue.ofMb(100).getBytes(), AllocationStatus.State.STARTED);
        long expectedTierBytes = (long) Math.ceil(
            ByteSizeValue.ofMb(100 + BASELINE_OVERHEAD_MB + 200 + BASELINE_OVERHEAD_MB).getBytes() * 100 / 30.0
        );
        long expectedNodeBytes = (long) Math.ceil(ByteSizeValue.ofMb(200 + BASELINE_OVERHEAD_MB).getBytes() * 100 / 30.0);

        assertMlCapacity(
            client().execute(GetAutoscalingCapacityAction.INSTANCE, new GetAutoscalingCapacityAction.Request()).actionGet(),
            "Requesting scale down as tier and/or node size could be smaller",
            expectedTierBytes,
            expectedNodeBytes
        );

        long modelSize = ByteSizeValue.ofMb(50_000).getBytes();
        putAndStartModelDeployment(modelId, modelSize, AllocationStatus.State.STARTING);

        List<DiscoveryNode> mlNodes = admin().cluster()
            .prepareNodesInfo()
            .all()
            .get()
            .getNodes()
            .stream()
            .map(NodeInfo::getNode)
            .filter(MachineLearning::isMlNode)
            .collect(Collectors.toList());
        NativeMemoryCapacity currentScale = MlAutoscalingDeciderService.currentScale(mlNodes, 30, false);
        expectedTierBytes = (long) Math.ceil(
            (ByteSizeValue.ofMb(50_000 + BASIC_REQUIREMENT_MB).getBytes() + currentScale.getTierMlNativeMemoryRequirement()) * 100 / 30.0
        );
        expectedNodeBytes = (long) (ByteSizeValue.ofMb(50_000 + BASELINE_OVERHEAD_MB).getBytes() * 100 / 30.0);

        assertMlCapacity(
            client().execute(GetAutoscalingCapacityAction.INSTANCE, new GetAutoscalingCapacityAction.Request()).actionGet(),
            "requesting scale up as number of jobs in queues exceeded configured limit "
                + "or there is at least one trained model waiting for allocation "
                + "and current capacity is not large enough for waiting jobs",
            expectedTierBytes,
            expectedNodeBytes
        );
    }

    private void assertMlCapacity(GetAutoscalingCapacityAction.Response capacity, String reason, long tierBytes, long nodeBytes) {
        assertThat(capacity.getResults(), hasKey("ml_test"));
        AutoscalingDeciderResults autoscalingDeciderResults = capacity.getResults().get("ml_test");
        assertThat(autoscalingDeciderResults.results(), hasKey("ml"));

        AutoscalingDeciderResult autoscalingDeciderResult = autoscalingDeciderResults.results().get("ml");
        assertThat(autoscalingDeciderResult.reason().summary(), containsString(reason));
        assertThat(autoscalingDeciderResult.requiredCapacity().total().memory().getBytes(), greaterThanOrEqualTo(tierBytes - 1L));
        assertThat(autoscalingDeciderResult.requiredCapacity().node().memory().getBytes(), greaterThanOrEqualTo(nodeBytes - 1L));
    }

    private void putJob(String jobId, long limitMb) {
        Job.Builder job = new Job.Builder(jobId).setAllowLazyOpen(true)
            .setAnalysisLimits(new AnalysisLimits(limitMb, null))
            .setAnalysisConfig(
                new AnalysisConfig.Builder((List<Detector>) null).setBucketSpan(TimeValue.timeValueHours(1))
                    .setDetectors(Collections.singletonList(new Detector.Builder("count", null).setPartitionFieldName("user").build()))
            )
            .setDataDescription(new DataDescription.Builder().setTimeFormat("epoch"));

        putJob(job);
    }

    private void putAndStartModelDeployment(String modelId, long memoryUse, AllocationStatus.State state) {
        client().execute(
            PutTrainedModelAction.INSTANCE,
            new PutTrainedModelAction.Request(
                TrainedModelConfig.builder()
                    .setModelType(TrainedModelType.PYTORCH)
                    .setInferenceConfig(
                        new PassThroughConfig(null, new BertTokenization(null, false, null, Tokenization.Truncate.NONE, -1), null)
                    )
                    .setModelId(modelId)
                    .build(),
                false
            )
        ).actionGet();
        client().execute(
            PutTrainedModelDefinitionPartAction.INSTANCE,
            new PutTrainedModelDefinitionPartAction.Request(
                modelId,
                new BytesArray(Base64.getDecoder().decode(BASE_64_ENCODED_MODEL)),
                0,
                memoryUse,
                1
            )
        ).actionGet();
        client().execute(
            PutTrainedModelVocabularyAction.INSTANCE,
            new PutTrainedModelVocabularyAction.Request(
                modelId,
                List.of("these", "are", "my", "words", BertTokenizer.UNKNOWN_TOKEN, BertTokenizer.PAD_TOKEN),
                List.of()
            )
        ).actionGet();
        client().execute(
            StartTrainedModelDeploymentAction.INSTANCE,
            new StartTrainedModelDeploymentAction.Request(modelId).setWaitForState(state)
        ).actionGet();
    }
}
