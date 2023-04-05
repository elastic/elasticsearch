/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests that involve interactions of ML jobs that are persistent tasks
 * and trained models.
 */
public class JobsAndModelsIT extends BaseMlIntegTestCase {

    public void testCluster_GivenAnomalyDetectionJobAndTrainedModelDeployment_ShouldNotAllocateBothOnSameNode() throws Exception {
        // This test starts 2 ML nodes and then starts an anomaly detection job and a
        // trained model deployment that do not both fit in one node. We then proceed
        // to stop both ML nodes and start a single ML node back up. We should see
        // that both the job and the model cannot be allocated on that node.

        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting dedicated master node...");
        internalCluster().startMasterOnlyNode();
        logger.info("Starting dedicated data node...");
        internalCluster().startDataOnlyNode();
        logger.info("Starting dedicated ml node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        logger.info("Starting dedicated ml node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();

        MlMemoryAction.Response memoryStats = client().execute(MlMemoryAction.INSTANCE, new MlMemoryAction.Request("ml:true")).actionGet();

        long maxNativeBytesPerNode = 0;
        for (MlMemoryAction.Response.MlMemoryStats stats : memoryStats.getNodes()) {
            maxNativeBytesPerNode = stats.getMlMax().getBytes();
        }

        String jobId = "test-node-goes-down-while-running-job";
        Job.Builder job = createJob(jobId, ByteSizeValue.ofBytes((long) (0.8 * maxNativeBytesPerNode)));

        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).actionGet();

        TrainedModelConfig model = TrainedModelConfig.builder()
            .setModelId("test_model")
            .setModelType(TrainedModelType.PYTORCH)
            .setModelSize((long) (0.3 * maxNativeBytesPerNode))
            .setInferenceConfig(new PassThroughConfig(new VocabularyConfig(InferenceIndexConstants.nativeDefinitionStore()), null, null))
            .setLocation(new IndexLocation(InferenceIndexConstants.nativeDefinitionStore()))
            .build();

        TrainedModelDefinitionDoc modelDefinitionDoc = new TrainedModelDefinitionDoc(
            new BytesArray(""),
            model.getModelId(),
            0,
            model.getModelSize(),
            model.getModelSize(),
            1,
            true
        );
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            modelDefinitionDoc.toXContent(builder, null);
            client().execute(
                IndexAction.INSTANCE,
                new IndexRequest(InferenceIndexConstants.nativeDefinitionStore()).source(builder)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();
        }

        client().execute(PutTrainedModelAction.INSTANCE, new PutTrainedModelAction.Request(model, true)).actionGet();
        client().execute(
            PutTrainedModelVocabularyAction.INSTANCE,
            new PutTrainedModelVocabularyAction.Request(
                model.getModelId(),
                List.of(
                    "these",
                    "are",
                    "my",
                    "words",
                    BertTokenizer.SEPARATOR_TOKEN,
                    BertTokenizer.CLASS_TOKEN,
                    BertTokenizer.UNKNOWN_TOKEN,
                    BertTokenizer.PAD_TOKEN
                ),
                List.of()
            )
        ).actionGet();

        logger.info("starting deployment: " + model.getModelId());
        client().execute(
            StartTrainedModelDeploymentAction.INSTANCE,
            new StartTrainedModelDeploymentAction.Request(model.getModelId(), model.getModelId())
        ).actionGet();

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        String jobNode = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId()))
            .actionGet()
            .getResponse()
            .results()
            .get(0)
            .getNode()
            .getName();
        String modelNode = client().execute(
            GetTrainedModelsStatsAction.INSTANCE,
            new GetTrainedModelsStatsAction.Request(model.getModelId())
        ).actionGet().getResources().results().get(0).getDeploymentStats().getNodeStats().get(0).getNode().getName();

        // Assert the job and model were assigned to different nodes as they would not fit in the same node
        assertThat(jobNode, not(equalTo(modelNode)));

        // Stop both ML nodes
        logger.info("Stopping both ml nodes...");
        assertThat(internalCluster().stopNode(jobNode), is(true));
        assertThat(internalCluster().stopNode(modelNode), is(true));

        // Wait for both the job and model to be unassigned
        assertBusy(() -> {
            GetJobsStatsAction.Response jobStats = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
            assertThat(jobStats.getResponse().results().get(0).getNode(), is(nullValue()));
        });
        assertBusy(() -> {
            GetTrainedModelsStatsAction.Response modelStats = client().execute(
                GetTrainedModelsStatsAction.INSTANCE,
                new GetTrainedModelsStatsAction.Request(model.getModelId())
            ).actionGet();
            assertThat(modelStats.getResources().results().get(0).getDeploymentStats().getNodeStats(), is(empty()));
        });

        // Start a new ML node
        logger.info("Starting dedicated ml node...");
        String lastMlNodeName = internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();

        // Wait until either the job or the model is assigned
        assertBusy(() -> {
            GetTrainedModelsStatsAction.Response modelStatsResponse = client().execute(
                GetTrainedModelsStatsAction.INSTANCE,
                new GetTrainedModelsStatsAction.Request(model.getModelId())
            ).actionGet();
            GetTrainedModelsStatsAction.Response.TrainedModelStats modelStats = modelStatsResponse.getResources().results().get(0);
            GetJobsStatsAction.Response jobStatsResponse = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
            GetJobsStatsAction.Response.JobStats jobStats = jobStatsResponse.getResponse().results().get(0);

            boolean isModelAssigned = modelStats.getDeploymentStats().getNodeStats().isEmpty() == false;
            boolean isJobAssigned = jobStats.getNode() != null;
            assertThat(isJobAssigned ^ isModelAssigned, is(true));

            if (isJobAssigned) {
                assertThat(jobStats.getNode().getName(), equalTo(lastMlNodeName));
                assertThat(modelStats.getDeploymentStats().getReason(), containsString("insufficient available memory"));
            } else {
                assertThat(modelStats.getDeploymentStats().getNodeStats().get(0).getNode().getName(), equalTo(lastMlNodeName));
                assertThat(jobStats.getAssignmentExplanation(), containsString("insufficient available memory"));
            }
        });

        // Start another new ML node
        logger.info("Starting dedicated ml node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();

        // Wait until both the job and the model are assigned
        // and check they are not on the same node
        assertBusy(() -> {
            GetTrainedModelsStatsAction.Response modelStatsResponse = client().execute(
                GetTrainedModelsStatsAction.INSTANCE,
                new GetTrainedModelsStatsAction.Request(model.getModelId())
            ).actionGet();
            GetTrainedModelsStatsAction.Response.TrainedModelStats modelStats = modelStatsResponse.getResources().results().get(0);
            assertThat(modelStats.getDeploymentStats().getNodeStats().isEmpty(), is(false));
            GetJobsStatsAction.Response jobStatsResponse = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
            GetJobsStatsAction.Response.JobStats jobStats = jobStatsResponse.getResponse().results().get(0);
            assertThat(jobStats.getNode(), is(notNullValue()));

            assertThat(jobStats.getNode(), is(not(equalTo(modelStats.getDeploymentStats().getNodeStats().get(0).getNode()))));
        });

        // Clean up
        client().execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId).setForce(true)).actionGet();
        client().execute(StopTrainedModelDeploymentAction.INSTANCE, new StopTrainedModelDeploymentAction.Request(model.getModelId()))
            .actionGet();
    }
}
