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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            new HashSet<>(
                Arrays.asList(
                    InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    ClusterService.USER_DEFINED_METADATA,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING
                )
            )
        );
        clusterService = new ClusterService(settings, clusterSettings, tp, null);
    }

    public void testCreateProcessorWithTooManyExisting() throws Exception {
        Set<Boolean> includeNodeInfoValues = new HashSet<>(Arrays.asList(true, false));

        includeNodeInfoValues.forEach(includeNodeInfo -> {
            InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
                client,
                clusterService,
                Settings.builder().put(InferenceProcessor.MAX_INFERENCE_PROCESSORS.getKey(), 1).build(),
                includeNodeInfo
            );

            try {
                processorFactory.accept(buildClusterStateWithModelReferences("model1"));
            } catch (IOException ioe) {
                throw new AssertionError(ioe.getMessage());
            }

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
        });
    }

    public void testCreateProcessorWithInvalidInferenceConfig() {
        Set<Boolean> includeNodeInfoValues = new HashSet<>(Arrays.asList(true, false));

        includeNodeInfoValues.forEach(includeNodeInfo -> {
            InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
                client,
                clusterService,
                Settings.EMPTY,
                includeNodeInfo
            );

            Map<String, Object> config = new HashMap<>() {
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
                equalTo(
                    "unrecognized inference configuration type [unknown_type]."
                        + " Supported types [classification, regression, fill_mask, ner, pass_through, "
                        + "question_answering, text_classification, text_embedding, text_expansion, "
                        + "text_similarity, zero_shot_classification]"
                )
            );

            Map<String, Object> config2 = new HashMap<>() {
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

            Map<String, Object> config3 = new HashMap<>() {
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
        });
    }

    public void testCreateProcessorWithTooOldMinNodeVersion() throws IOException {
        Set<Boolean> includeNodeInfoValues = new HashSet<>(Arrays.asList(true, false));

        includeNodeInfoValues.forEach(includeNodeInfo -> {
            InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
                client,
                clusterService,
                Settings.EMPTY,
                includeNodeInfo
            );
            try {
                processorFactory.accept(builderClusterStateWithModelReferences(Version.V_7_5_0, "model1"));
            } catch (IOException ioe) {
                throw new AssertionError(ioe.getMessage());
            }
            Map<String, Object> regression = new HashMap<>() {
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

            Map<String, Object> classification = new HashMap<>() {
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
        });
    }

    public void testCreateProcessorWithTooOldMinNodeVersionNlp() throws IOException {
        Set<Boolean> includeNodeInfoValues = new HashSet<>(Arrays.asList(true, false));

        includeNodeInfoValues.forEach(includeNodeInfo -> {
            InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
                client,
                clusterService,
                Settings.EMPTY,
                includeNodeInfo
            );
            try {
                processorFactory.accept(builderClusterStateWithModelReferences(Version.V_7_5_0, "model1"));
            } catch (IOException ioe) {
                throw new AssertionError(ioe.getMessage());
            }

            for (String name : List.of(
                FillMaskConfig.NAME,
                NerConfig.NAME,
                PassThroughConfig.NAME,
                QuestionAnsweringConfig.NAME,
                TextClassificationConfig.NAME,
                TextEmbeddingConfig.NAME,
                TextExpansionConfigUpdate.NAME,
                TextSimilarityConfig.NAME,
                ZeroShotClassificationConfig.NAME
            )) {
                ElasticsearchException ex = expectThrows(
                    ElasticsearchException.class,
                    () -> processorFactory.inferenceConfigUpdateFromMap(Map.of(name, Map.of()))
                );
                assertThat(
                    ex.getMessage(),
                    equalTo("Configuration [" + name + "] requires minimum node version [8.0.0] (current minimum node version [7.5.0]")
                );
            }

            for (String name : List.of(ClassificationConfig.NAME.getPreferredName(), RegressionConfig.NAME.getPreferredName())) {
                ElasticsearchException ex = expectThrows(
                    ElasticsearchException.class,
                    () -> processorFactory.inferenceConfigUpdateFromMap(Map.of(name, Map.of()))
                );
                assertThat(
                    ex.getMessage(),
                    equalTo("Configuration [" + name + "] requires minimum node version [7.6.0] (current minimum node version [7.5.0]")
                );
            }
        });
    }

    public void testCreateProcessorWithEmptyConfigNotSupportedOnOldNode() throws IOException {
        Set<Boolean> includeNodeInfoValues = new HashSet<>(Arrays.asList(true, false));

        includeNodeInfoValues.forEach(includeNodeInfo -> {
            InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
                client,
                clusterService,
                Settings.EMPTY,
                includeNodeInfo
            );
            try {
                processorFactory.accept(builderClusterStateWithModelReferences(Version.V_7_5_0, "model1"));
            } catch (IOException ioe) {
                throw new AssertionError(ioe.getMessage());
            }

            Map<String, Object> minimalConfig = new HashMap<>() {
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
        });
    }

    public void testCreateProcessor() {
        Set<Boolean> includeNodeInfoValues = new HashSet<>(Arrays.asList(true, false));

        includeNodeInfoValues.forEach(includeNodeInfo -> {
            InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
                client,
                clusterService,
                Settings.EMPTY,
                includeNodeInfo
            );

            Map<String, Object> regression = new HashMap<>() {
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

            Map<String, Object> classification = new HashMap<>() {
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

            Map<String, Object> mininmal = new HashMap<>() {
                {
                    put(InferenceResults.MODEL_ID_RESULTS_FIELD, "my_model");
                    put(InferenceProcessor.TARGET_FIELD, "result");
                }
            };

            processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, mininmal);
        });
    }

    public void testCreateProcessorWithDuplicateFields() {
        Set<Boolean> includeNodeInfoValues = new HashSet<>(Arrays.asList(true, false));

        includeNodeInfoValues.forEach(includeNodeInfo -> {
            InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
                client,
                clusterService,
                Settings.EMPTY,
                includeNodeInfo
            );

            Map<String, Object> regression = new HashMap<>() {
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
        });
    }

    public void testParseFromMap() {
        Set<Boolean> includeNodeInfoValues = new HashSet<>(Arrays.asList(true, false));

        includeNodeInfoValues.forEach(includeNodeInfo -> {
            InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
                client,
                clusterService,
                Settings.EMPTY,
                includeNodeInfo
            );
            for (var nameAndMap : List.of(
                Tuple.tuple(ClassificationConfig.NAME.getPreferredName(), Map.of()),
                Tuple.tuple(RegressionConfig.NAME.getPreferredName(), Map.of()),
                Tuple.tuple(FillMaskConfig.NAME, Map.of()),
                Tuple.tuple(NerConfig.NAME, Map.of()),
                Tuple.tuple(PassThroughConfig.NAME, Map.of()),
                Tuple.tuple(TextClassificationConfig.NAME, Map.of()),
                Tuple.tuple(TextEmbeddingConfig.NAME, Map.of()),
                Tuple.tuple(ZeroShotClassificationConfig.NAME, Map.of()),
                Tuple.tuple(QuestionAnsweringConfig.NAME, Map.of("question", "What is the answer to life, the universe and everything?"))
            )) {
                assertThat(
                    processorFactory.inferenceConfigUpdateFromMap(Map.of(nameAndMap.v1(), nameAndMap.v2())).getName(),
                    equalTo(nameAndMap.v1())
                );
            }
        });
    }

    private static ClusterState buildClusterState(Metadata metadata) {
        return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
    }

    private static ClusterState buildClusterStateWithModelReferences(String... modelId) throws IOException {
        return builderClusterStateWithModelReferences(Version.CURRENT, modelId);
    }

    private static ClusterState builderClusterStateWithModelReferences(Version minNodeVersion, String... modelId) throws IOException {
        Map<String, PipelineConfiguration> configurations = Maps.newMapWithExpectedSize(modelId.length);
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
                    .add(
                        DiscoveryNodeUtils.create("min_node", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), minNodeVersion)
                    )
                    .add(DiscoveryNodeUtils.create("current_node", new TransportAddress(InetAddress.getLoopbackAddress(), 9302)))
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9304)))
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
        return Collections.singletonMap("foreach", new HashMap<>() {
            {
                put("field", "foo");
                put("processor", inferenceProcessorForModel(modelId));
            }
        });
    }

    private static Map<String, Object> inferenceProcessorForModel(String modelId) {
        return Collections.singletonMap(InferenceProcessor.TYPE, new HashMap<>() {
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
