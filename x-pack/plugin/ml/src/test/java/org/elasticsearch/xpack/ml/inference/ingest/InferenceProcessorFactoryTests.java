/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfigUpdate;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceProcessorFactoryTests extends ESTestCase {

    private Client client;
    private ClusterService clusterService;

    @Before
    public void setUpVariables() {
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(tp.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
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
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_THREAD_DUMP_TIMEOUT_SETTING
                )
            )
        );
        clusterService = new ClusterService(settings, clusterSettings, tp, null);
    }

    public void testCreateProcessorWithTooManyExisting() {
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
                    put(InferenceProcessor.MODEL_ID, "my_model");
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
                    put(InferenceProcessor.MODEL_ID, "my_model");
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
                    put(InferenceProcessor.MODEL_ID, "my_model");
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

    public void testCreateProcessorWithTooOldMinNodeVersion() {
        Set<Boolean> includeNodeInfoValues = new HashSet<>(Arrays.asList(true, false));

        includeNodeInfoValues.forEach(includeNodeInfo -> {
            InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
                client,
                clusterService,
                Settings.EMPTY,
                includeNodeInfo
            );
            try {
                processorFactory.accept(builderClusterStateWithModelReferences(MlConfigVersion.V_7_5_0, "model1"));
            } catch (IOException ioe) {
                throw new AssertionError(ioe.getMessage());
            }
            Map<String, Object> regression = new HashMap<>() {
                {
                    put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                    put(InferenceProcessor.MODEL_ID, "my_model");
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
                    put(InferenceProcessor.MODEL_ID, "my_model");
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
                processorFactory.accept(builderClusterStateWithModelReferences(MlConfigVersion.V_7_5_0, "model1"));
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
                    put(InferenceProcessor.MODEL_ID, "my_model");
                    put(InferenceProcessor.TARGET_FIELD, "result");
                    put(
                        InferenceProcessor.INFERENCE_CONFIG,
                        Collections.singletonMap(RegressionConfig.NAME.getPreferredName(), Collections.emptyMap())
                    );
                }
            };

            var processor = processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, regression);
            assertEquals(includeNodeInfo, processor.getAuditor().includeNodeInfo());
            assertFalse(processor.isConfiguredWithInputsFields());
            assertEquals("my_model", processor.getModelId());
            assertEquals("result", processor.getTargetField());
            assertThat(processor.getFieldMap().entrySet(), empty());
            assertNull(processor.getInputs());

            Map<String, Object> classification = new HashMap<>() {
                {
                    put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                    put(InferenceProcessor.MODEL_ID, "my_model");
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

            processor = processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, classification);
            assertFalse(processor.isConfiguredWithInputsFields());

            Map<String, Object> mininmal = new HashMap<>() {
                {
                    put(InferenceProcessor.MODEL_ID, "my_model");
                    put(InferenceProcessor.TARGET_FIELD, "result");
                }
            };

            processor = processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, mininmal);
            assertFalse(processor.isConfiguredWithInputsFields());
            assertEquals("my_model", processor.getModelId());
            assertEquals("result", processor.getTargetField());
            assertNull(processor.getInputs());
        });
    }

    public void testCreateProcessorWithFieldMap() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client, clusterService, Settings.EMPTY, false);

        Map<String, Object> config = new HashMap<>() {
            {
                put(InferenceProcessor.FIELD_MAP, Collections.singletonMap("source", "dest"));
                put(InferenceProcessor.MODEL_ID, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "result");
                put(
                    InferenceProcessor.INFERENCE_CONFIG,
                    Collections.singletonMap(RegressionConfig.NAME.getPreferredName(), Collections.emptyMap())
                );
            }
        };

        var processor = processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, config);
        assertFalse(processor.isConfiguredWithInputsFields());
        assertEquals("my_model", processor.getModelId());
        assertEquals("result", processor.getTargetField());
        assertNull(processor.getInputs());
        var fieldMap = processor.getFieldMap();
        assertThat(fieldMap.entrySet(), hasSize(1));
        assertThat(fieldMap, hasEntry("source", "dest"));
    }

    public void testCreateProcessorWithInputOutputs() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client, clusterService, Settings.EMPTY, false);

        Map<String, Object> config = new HashMap<>();
        config.put(InferenceProcessor.MODEL_ID, "my_model");

        Map<String, Object> input1 = new HashMap<>();
        input1.put(InferenceProcessor.INPUT_FIELD, "in1");
        input1.put(InferenceProcessor.OUTPUT_FIELD, "out1");
        Map<String, Object> input2 = new HashMap<>();
        input2.put(InferenceProcessor.INPUT_FIELD, "in2");
        input2.put(InferenceProcessor.OUTPUT_FIELD, "out2");

        List<Map<String, Object>> inputOutputs = new ArrayList<>();
        inputOutputs.add(input1);
        inputOutputs.add(input2);
        config.put(InferenceProcessor.INPUT_OUTPUT, inputOutputs);

        var processor = processorFactory.create(Collections.emptyMap(), "my_inference_processor", null, config);
        assertTrue(processor.isConfiguredWithInputsFields());
        assertEquals("my_model", processor.getModelId());
        var configuredInputs = processor.getInputs();
        assertThat(configuredInputs, hasSize(2));
        assertEquals(configuredInputs.get(0).inputField(), "in1");
        assertEquals(configuredInputs.get(0).outputField(), "out1");
        assertEquals(configuredInputs.get(1).inputField(), "in2");
        assertEquals(configuredInputs.get(1).outputField(), "out2");

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
                    put(InferenceProcessor.MODEL_ID, "my_model");
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

    public void testCreateProcessorWithIgnoreMissing() {
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
                    put(InferenceProcessor.MODEL_ID, "my_model");
                    put(InferenceProcessor.FIELD_MAP, Collections.emptyMap());
                    put("ignore_missing", Boolean.TRUE);
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

    public void testParseInferenceConfigFromMap() {
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
                Tuple.tuple(TextExpansionConfig.NAME, Map.of()),
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

    public void testCreateProcessorWithIncompatibleTargetFieldSetting() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
            client,
            clusterService,
            Settings.EMPTY,
            randomBoolean()
        );

        Map<String, Object> input = new HashMap<>() {
            {
                put(InferenceProcessor.INPUT_FIELD, "in");
                put(InferenceProcessor.OUTPUT_FIELD, "out");
            }
        };

        Map<String, Object> config = new HashMap<>() {
            {
                put(InferenceProcessor.MODEL_ID, "my_model");
                put(InferenceProcessor.TARGET_FIELD, "ml");
                put(InferenceProcessor.INPUT_OUTPUT, List.of(input));
            }
        };

        ElasticsearchParseException ex = expectThrows(
            ElasticsearchParseException.class,
            () -> processorFactory.create(Collections.emptyMap(), "processor_with_inputs", null, config)
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "[target_field] option is incompatible with [input_output]. Use the [output_field] option to specify where to write the "
                    + "inference results to."
            )
        );
    }

    public void testCreateProcessorWithIncompatibleResultFieldSetting() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
            client,
            clusterService,
            Settings.EMPTY,
            randomBoolean()
        );

        Map<String, Object> input = new HashMap<>() {
            {
                put(InferenceProcessor.INPUT_FIELD, "in");
                put(InferenceProcessor.OUTPUT_FIELD, "out");
            }
        };

        Map<String, Object> config = new HashMap<>() {
            {
                put(InferenceProcessor.MODEL_ID, "my_model");
                put(InferenceProcessor.INPUT_OUTPUT, List.of(input));
                put(
                    InferenceProcessor.INFERENCE_CONFIG,
                    Collections.singletonMap(
                        TextExpansionConfig.NAME,
                        Collections.singletonMap(TextExpansionConfig.RESULTS_FIELD.getPreferredName(), "foo")
                    )
                );
            }
        };

        ElasticsearchParseException ex = expectThrows(
            ElasticsearchParseException.class,
            () -> processorFactory.create(Collections.emptyMap(), "processor_with_inputs", null, config)
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "The [inference_config.results_field] setting is incompatible with using [input_output]. "
                    + "Prefer to use the [input_output.output_field] option to specify where to write the inference results to."
            )
        );
    }

    public void testCreateProcessorWithInputFields() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
            client,
            clusterService,
            Settings.EMPTY,
            randomBoolean()
        );

        Map<String, Object> inputMap1 = new HashMap<>() {
            {
                put(InferenceProcessor.INPUT_FIELD, "in1");
                put(InferenceProcessor.OUTPUT_FIELD, "out1");
            }
        };
        Map<String, Object> inputMap2 = new HashMap<>() {
            {
                put(InferenceProcessor.INPUT_FIELD, "in2");
                put(InferenceProcessor.OUTPUT_FIELD, "out2");
            }
        };

        String inferenceConfigType = randomFrom(
            ClassificationConfigUpdate.NAME.getPreferredName(),
            RegressionConfigUpdate.NAME.getPreferredName(),
            FillMaskConfigUpdate.NAME,
            NerConfigUpdate.NAME,
            PassThroughConfigUpdate.NAME,
            QuestionAnsweringConfigUpdate.NAME,
            TextClassificationConfigUpdate.NAME,
            TextEmbeddingConfigUpdate.NAME,
            TextExpansionConfigUpdate.NAME,
            TextSimilarityConfigUpdate.NAME,
            ZeroShotClassificationConfigUpdate.NAME
        );

        Map<String, Object> config = new HashMap<>() {
            {
                put(InferenceProcessor.MODEL_ID, "my_model");
                put(InferenceProcessor.INPUT_OUTPUT, List.of(inputMap1, inputMap2));
            }
        };
        // create valid inference configs with required fields
        if (inferenceConfigType.equals(TextSimilarityConfigUpdate.NAME)) {
            var inferenceConfig = new HashMap<String, String>();
            inferenceConfig.put(TextSimilarityConfig.TEXT.getPreferredName(), "text to compare");
            config.put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap(inferenceConfigType, inferenceConfig));
        } else if (inferenceConfigType.equals(QuestionAnsweringConfigUpdate.NAME)) {
            var inferenceConfig = new HashMap<String, String>();
            inferenceConfig.put(QuestionAnsweringConfig.QUESTION.getPreferredName(), "why is the sky blue?");
            config.put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap(inferenceConfigType, inferenceConfig));
        } else {
            config.put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap(inferenceConfigType, Collections.emptyMap()));
        }

        var inferenceProcessor = processorFactory.create(Collections.emptyMap(), "processor_with_inputs", null, config);
        assertEquals("my_model", inferenceProcessor.getModelId());
        assertTrue(inferenceProcessor.isConfiguredWithInputsFields());

        var inputs = inferenceProcessor.getInputs();
        assertThat(inputs, hasSize(2));
        assertEquals(inputs.get(0), new InferenceProcessor.Factory.InputConfig("in1", null, "out1", Map.of()));
        assertEquals(inputs.get(1), new InferenceProcessor.Factory.InputConfig("in2", null, "out2", Map.of()));

        assertNull(inferenceProcessor.getFieldMap());
        assertNull(inferenceProcessor.getTargetField());
    }

    public void testCreateProcessorWithInputFieldSingleOrList() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
            client,
            clusterService,
            Settings.EMPTY,
            randomBoolean()
        );

        for (var isList : new boolean[] { true, false }) {
            Map<String, Object> inputMap = new HashMap<>() {
                {
                    put(InferenceProcessor.INPUT_FIELD, "in");
                    put(InferenceProcessor.OUTPUT_FIELD, "out");
                }
            };

            Map<String, Object> config = new HashMap<>();
            config.put(InferenceProcessor.MODEL_ID, "my_model");
            if (isList) {
                config.put(InferenceProcessor.INPUT_OUTPUT, List.of(inputMap));
            } else {
                config.put(InferenceProcessor.INPUT_OUTPUT, inputMap);
            }

            if (randomBoolean()) {
                config.put(
                    InferenceProcessor.INFERENCE_CONFIG,
                    Collections.singletonMap(TextExpansionConfigUpdate.NAME, Collections.emptyMap())
                );
            }

            var inferenceProcessor = processorFactory.create(Collections.emptyMap(), "processor_with_single_input", null, config);
            assertEquals("my_model", inferenceProcessor.getModelId());
            assertTrue(inferenceProcessor.isConfiguredWithInputsFields());

            var inputs = inferenceProcessor.getInputs();
            assertThat(inputs, hasSize(1));
            assertEquals(inputs.get(0), new InferenceProcessor.Factory.InputConfig("in", null, "out", Map.of()));

            assertNull(inferenceProcessor.getFieldMap());
            assertNull(inferenceProcessor.getTargetField());
        }
    }

    public void testCreateProcessorWithInputFieldWrongType() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
            client,
            clusterService,
            Settings.EMPTY,
            randomBoolean()
        );

        {
            Map<String, Object> config = new HashMap<>();
            config.put(InferenceProcessor.MODEL_ID, "my_model");
            config.put(InferenceProcessor.INPUT_OUTPUT, List.of(1, 2, 3));

            var e = expectThrows(
                ElasticsearchParseException.class,
                () -> processorFactory.create(Collections.emptyMap(), "processor_with_bad_config", null, config)
            );
            assertThat(e.getMessage(), containsString("[input_output] property isn't a list of maps"));
        }
        {
            Map<String, Object> config = new HashMap<>();
            config.put(InferenceProcessor.MODEL_ID, "my_model");
            config.put(InferenceProcessor.INPUT_OUTPUT, Boolean.TRUE);

            var e = expectThrows(
                ElasticsearchParseException.class,
                () -> processorFactory.create(Collections.emptyMap(), "processor_with_bad_config", null, config)
            );
            assertThat(e.getMessage(), containsString("[input_output] property isn't a map or list of maps"));
        }
        {
            Map<Boolean, String> badMap = new HashMap<>();
            badMap.put(Boolean.TRUE, "foo");
            Map<String, Object> config = new HashMap<>();
            config.put(InferenceProcessor.MODEL_ID, "my_model");
            config.put(InferenceProcessor.INPUT_OUTPUT, badMap);

            var e = expectThrows(
                ElasticsearchParseException.class,
                () -> processorFactory.create(Collections.emptyMap(), "processor_with_bad_config", null, config)
            );
            assertThat(e.getMessage(), containsString("[input_field] required property is missing"));
        }
        {
            // empty list
            Map<String, Object> config = new HashMap<>();
            config.put(InferenceProcessor.MODEL_ID, "my_model");
            config.put(InferenceProcessor.INPUT_OUTPUT, List.of());

            var e = expectThrows(
                ElasticsearchParseException.class,
                () -> processorFactory.create(Collections.emptyMap(), "processor_with_bad_config", null, config)
            );
            assertThat(e.getMessage(), containsString("[input_output] property cannot be empty at least one is required"));
        }
    }

    public void testParsingInputFields() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
            client,
            clusterService,
            Settings.EMPTY,
            randomBoolean()
        );

        int numInputs = randomIntBetween(1, 3);
        List<Map<String, Object>> inputs = new ArrayList<>();
        for (int i = 0; i < numInputs; i++) {
            Map<String, Object> inputMap = new HashMap<>();
            inputMap.put(InferenceProcessor.INPUT_FIELD, "in" + i);
            inputMap.put(InferenceProcessor.OUTPUT_FIELD, "out." + i);
            inputs.add(inputMap);
        }

        var parsedInputs = processorFactory.parseInputFields("my_processor", inputs);
        assertThat(parsedInputs, hasSize(numInputs));
        for (int i = 0; i < numInputs; i++) {
            assertEquals(new InferenceProcessor.Factory.InputConfig("in" + i, "out", Integer.toString(i), Map.of()), parsedInputs.get(i));
        }
    }

    public void testParsingInputFieldsDuplicateFieldNames() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
            client,
            clusterService,
            Settings.EMPTY,
            randomBoolean()
        );

        int numInputs = 2;
        {
            List<Map<String, Object>> inputs = new ArrayList<>();
            for (int i = 0; i < numInputs; i++) {
                Map<String, Object> inputMap = new HashMap<>();
                inputMap.put(InferenceProcessor.INPUT_FIELD, "in");
                inputMap.put(InferenceProcessor.OUTPUT_FIELD, "out" + i);
                inputs.add(inputMap);
            }

            var e = expectThrows(ElasticsearchParseException.class, () -> processorFactory.parseInputFields("my_processor", inputs));
            assertThat(e.getMessage(), containsString("[input_field] names must be unique but [in] is repeated"));
        }

        {
            List<Map<String, Object>> inputs = new ArrayList<>();
            for (int i = 0; i < numInputs; i++) {
                Map<String, Object> inputMap = new HashMap<>();
                inputMap.put(InferenceProcessor.INPUT_FIELD, "in" + i);
                inputMap.put(InferenceProcessor.OUTPUT_FIELD, "out");
                inputs.add(inputMap);
            }

            var e = expectThrows(ElasticsearchParseException.class, () -> processorFactory.parseInputFields("my_processor", inputs));
            assertThat(e.getMessage(), containsString("[output_field] names must be unique but [out] is repeated"));
        }
    }

    public void testExtractBasePathAndFinalElement() {
        {
            String path = "foo.bar.result";
            var extractedPaths = InferenceProcessor.Factory.extractBasePathAndFinalElement(path);
            assertEquals("foo.bar", extractedPaths.v1());
            assertEquals("result", extractedPaths.v2());
        }

        {
            String path = "result";
            var extractedPaths = InferenceProcessor.Factory.extractBasePathAndFinalElement(path);
            assertNull(extractedPaths.v1());
            assertEquals("result", extractedPaths.v2());
        }
    }

    public void testParsingInputFieldsGivenNoInputs() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(
            client,
            clusterService,
            Settings.EMPTY,
            randomBoolean()
        );

        var e = expectThrows(ElasticsearchParseException.class, () -> processorFactory.parseInputFields("my_processor", List.of()));
        assertThat(e.getMessage(), containsString("[input_output] property cannot be empty at least one is required"));
    }

    private static ClusterState buildClusterStateWithModelReferences(String... modelId) throws IOException {
        return builderClusterStateWithModelReferences(MlConfigVersion.CURRENT, modelId);
    }

    private static ClusterState builderClusterStateWithModelReferences(MlConfigVersion minNodeVersion, String... modelId)
        throws IOException {
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
                        DiscoveryNodeUtils.create(
                            "min_node",
                            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                            Map.of(MachineLearning.ML_CONFIG_VERSION_NODE_ATTR, minNodeVersion.toString()),
                            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.ML_ROLE, DiscoveryNodeRole.DATA_ROLE)
                        )
                    )
                    .add(
                        DiscoveryNodeUtils.create(
                            "current_node",
                            new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                            Map.of(MachineLearning.ML_CONFIG_VERSION_NODE_ATTR, MlConfigVersion.CURRENT.toString()),
                            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.ML_ROLE, DiscoveryNodeRole.DATA_ROLE)
                        )
                    )
                    .add(
                        DiscoveryNodeUtils.create(
                            "_node_id",
                            new TransportAddress(InetAddress.getLoopbackAddress(), 9304),
                            Map.of(MachineLearning.ML_CONFIG_VERSION_NODE_ATTR, MlConfigVersion.CURRENT.toString()),
                            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.ML_ROLE, DiscoveryNodeRole.DATA_ROLE)
                        )
                    )
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
                put(InferenceProcessor.MODEL_ID, modelId);
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
