/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceProcessorFactoryTests extends ESTestCase {

    private Client client;
    private ClusterService clusterService;

    @Before
    public void setUpVariables() {
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        client = mock(Client.class);
        Settings settings = Settings.builder().put("node.name", "InferenceProcessorFactoryTests_node").build();
        when(client.settings()).thenReturn(settings);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            new HashSet<>(
                Arrays.asList(
                    InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    ClusterService.USER_DEFINED_METADATA,
                    AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING
                )
            )
        );
        clusterService = new ClusterService(settings, clusterSettings, tp);
    }

    public void testCreateProcessorWithTooManyExisting() throws Exception {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
            client,
            clusterService,
            Settings.builder().put(InferenceProcessor.MAX_INFERENCE_PROCESSORS.getKey(), 1).build()
        );

        processorFactory.accept(buildClusterStateWithModelReferences("model1"));

        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, Collections.emptyMap())
        );

        assertThat(
            ex.getMessage(),
            equalTo(
                "Max number of inference processors reached, total inference processors [1]. "
                    + "Adjust the setting [xpack.ml.max_inference_processors]: [1] if a greater number is desired."
            )
        );
    }

    public void testCreateProcessorWithInvalidInferenceConfig() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client, clusterService, Settings.EMPTY);

        Map<String, Object> config = new HashMap<String, Object>() {
            {
                put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
                put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap("unknown_type", Collections.emptyMap()));
            }
        };

        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, config)
        );
        assertThat(
            ex.getMessage(),
            equalTo("unrecognized inference configuration type [unknown_type]. Supported types [classification, regression]")
        );

        Map<String, Object> config2 = new HashMap<String, Object>() {
            {
                put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
                put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap("regression", "boom"));
            }
        };
        ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, config2)
        );
        assertThat(ex.getMessage(), equalTo("inference_config must be an object with one inference type mapped to an object."));

        Map<String, Object> config3 = new HashMap<String, Object>() {
            {
                put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
                put(InferenceProcessor.INFERENCE_CONFIG, Collections.emptyMap());
            }
        };
        ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, config3)
        );
        assertThat(ex.getMessage(), equalTo("inference_config must be an object with one inference type mapped to an object."));
    }

    public void testCreateProcessorWithTooOldMinNodeVersion() throws IOException {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client, clusterService, Settings.EMPTY);
        processorFactory.accept(builderClusterStateWithModelReferences(Version.V_7_5_0, "model1"));

        Map<String, Object> regression = new HashMap<String, Object>() {
            {
                put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
                put(
                    InferenceProcessor.INFERENCE_CONFIG,
                    Collections.singletonMap(RegressionConfig.NAME.getPreferredName(), Collections.emptyMap())
                );
            }
        };

        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, regression)
        );
        assertThat(
            ex.getMessage(),
            equalTo("Configuration [regression] requires minimum node version [7.6.0] (current minimum node version [7.5.0]")
        );

        Map<String, Object> classification = new HashMap<String, Object>() {
            {
                put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
                put(
                    InferenceProcessor.INFERENCE_CONFIG,
                    Collections.singletonMap(
                        ClassificationConfig.NAME.getPreferredName(),
                        Collections.singletonMap(ClassificationConfig.NUM_TOP_CLASSES.getPreferredName(), 1)
                    )
                );
            }
        };

        ex = expectThrows(
            ElasticsearchException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, classification)
        );
        assertThat(
            ex.getMessage(),
            equalTo("Configuration [classification] requires minimum node version [7.6.0] (current minimum node version [7.5.0]")
        );
    }

    public void testCreateProcessorWithEmptyConfigNotSupportedOnOldNode() throws IOException {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client, clusterService, Settings.EMPTY);
        processorFactory.accept(builderClusterStateWithModelReferences(Version.V_7_5_0, "model1"));

        Map<String, Object> minimalConfig = new HashMap<String, Object>() {
            {
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
            }
        };

        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, minimalConfig)
        );
        assertThat(ex.getMessage(), equalTo("[inference_config] required property is missing"));
    }

    public void testCreateProcessor() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client, clusterService, Settings.EMPTY);

        Map<String, Object> regression = new HashMap<String, Object>() {
            {
                put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
                put(
                    InferenceProcessor.INFERENCE_CONFIG,
                    Collections.singletonMap(RegressionConfig.NAME.getPreferredName(), Collections.emptyMap())
                );
            }
        };

        processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, regression);

        Map<String, Object> classification = new HashMap<String, Object>() {
            {
                put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
                put(
                    InferenceProcessor.INFERENCE_CONFIG,
                    Collections.singletonMap(
                        ClassificationConfig.NAME.getPreferredName(),
                        Collections.singletonMap(ClassificationConfig.NUM_TOP_CLASSES.getPreferredName(), 1)
                    )
                );
            }
        };

        processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, classification);

        Map<String, Object> mininmal = new HashMap<String, Object>() {
            {
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
            }
        };

        processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, mininmal);
    }

    public void testCreateProcessorWithDuplicateFields() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client, clusterService, Settings.EMPTY);

        Map<String, Object> regression = new HashMap<String, Object>() {
            {
                put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "ml");
                put(
                    InferenceProcessor.INFERENCE_CONFIG,
                    Collections.singletonMap(
                        RegressionConfig.NAME.getPreferredName(),
                        Collections.singletonMap(RegressionConfig.RESULTS_FIELD.getPreferredName(), "warning")
                    )
                );
            }
        };

        Exception ex = expectThrows(
            Exception.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, regression)
        );
        assertThat(ex.getMessage(), equalTo("Invalid inference config. " + "More than one field is configured as [warning]"));
    }

    private static ClusterState buildClusterStateWithModelReferences(String... modelId) throws IOException {
        return builderClusterStateWithModelReferences(Version.CURRENT, modelId);
    }

    private static ClusterState builderClusterStateWithModelReferences(Version minNodeVersion, String... modelId) throws IOException {
        Map<String, PipelineConfiguration> configurations = new HashMap<>(modelId.length);
        for (String id : modelId) {
            configurations.put(
                "pipeline_with_model_" + id,
                randomBoolean() ? newConfigurationWithInferenceProcessor(id) : newConfigurationWithForeachProcessorProcessor(id)
            );
        }
        IngestMetadata ingestMetadata = new IngestMetadata(configurations);

        return ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("min_node", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), minNodeVersion))
                    .add(new DiscoveryNode("current_node", new TransportAddress(InetAddress.getLoopbackAddress(), 9302), Version.CURRENT))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();
    }

    private static PipelineConfiguration newConfigurationWithInferenceProcessor(String modelId) throws IOException {
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .map(Collections.singletonMap("processors", Collections.singletonList(inferenceProcessorForModel(modelId))))
        ) {
            return new PipelineConfiguration("pipeline_with_model_" + modelId, BytesReference.bytes(xContentBuilder), XContentType.JSON);
        }
    }

    private static PipelineConfiguration newConfigurationWithForeachProcessorProcessor(String modelId) throws IOException {
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .map(Collections.singletonMap("processors", Collections.singletonList(forEachProcessorWithInference(modelId))))
        ) {
            return new PipelineConfiguration("pipeline_with_model_" + modelId, BytesReference.bytes(xContentBuilder), XContentType.JSON);
        }
    }

    private static Map<String, Object> forEachProcessorWithInference(String modelId) {
        return Collections.singletonMap("foreach", new HashMap<String, Object>() {
            {
                put("field", "foo");
                put("processor", inferenceProcessorForModel(modelId));
            }
        });
    }

    private static Map<String, Object> inferenceProcessorForModel(String modelId) {
        return Collections.singletonMap(InferenceProcessor.TYPE, new HashMap<String, Object>() {
            {
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, modelId);
                put(
                    InferenceProcessor.INFERENCE_CONFIG,
                    Collections.singletonMap(RegressionConfig.NAME.getPreferredName(), Collections.emptyMap())
                );
                put(InferenceProcessor.TARGET_FIELD, "new_field");
                put(InferenceProcessor.FIELD_MAP, Collections.singletonMap("source", "dest"));
            }
        });
    }

}
