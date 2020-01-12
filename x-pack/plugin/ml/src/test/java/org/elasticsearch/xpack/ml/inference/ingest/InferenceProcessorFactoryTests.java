/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
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
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceProcessorFactoryTests extends ESTestCase {

    private static final IngestPlugin SKINNY_PLUGIN = new IngestPlugin() {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            XPackLicenseState licenseState = mock(XPackLicenseState.class);
            when(licenseState.isMachineLearningAllowed()).thenReturn(true);
            return Collections.singletonMap(InferenceProcessor.TYPE,
                new InferenceProcessor.Factory(parameters.client,
                    parameters.ingestService.getClusterService(),
                    Settings.EMPTY,
                    parameters.ingestService));
        }
    };
    private Client client;
    private XPackLicenseState licenseState;
    private ClusterService clusterService;
    private IngestService ingestService;

    @Before
    public void setUpVariables() {
        ThreadPool tp = mock(ThreadPool.class);
        ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(tp.generic()).thenReturn(executorService);
        client = mock(Client.class);
        Settings settings = Settings.builder().put("node.name", "InferenceProcessorFactoryTests_node").build();
        ClusterSettings clusterSettings = new ClusterSettings(settings,
            new HashSet<>(Arrays.asList(InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                ClusterService.USER_DEFINED_META_DATA,
                ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING)));
        clusterService = new ClusterService(settings, clusterSettings, tp);
        ingestService = new IngestService(clusterService, tp, null, null,
            null, Collections.singletonList(SKINNY_PLUGIN), client);
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isMachineLearningAllowed()).thenReturn(true);
    }

    public void testNumInferenceProcessors() throws Exception {
        MetaData metaData = null;

        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client,
            clusterService,
            Settings.EMPTY,
            ingestService);
        processorFactory.accept(buildClusterState(metaData));

        assertThat(processorFactory.numInferenceProcessors(), equalTo(0));
        metaData = MetaData.builder().build();

        processorFactory.accept(buildClusterState(metaData));
        assertThat(processorFactory.numInferenceProcessors(), equalTo(0));

        processorFactory.accept(buildClusterStateWithModelReferences("model1", "model2", "model3"));
        assertThat(processorFactory.numInferenceProcessors(), equalTo(3));
    }

    public void testCreateProcessorWithTooManyExisting() throws Exception {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client,
            clusterService,
            Settings.builder().put(InferenceProcessor.MAX_INFERENCE_PROCESSORS.getKey(), 1).build(),
            ingestService);

        processorFactory.accept(buildClusterStateWithModelReferences("model1"));

        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", Collections.emptyMap()));

        assertThat(ex.getMessage(), equalTo("Max number of inference processors reached, total inference processors [1]. " +
            "Adjust the setting [xpack.ml.max_inference_processors]: [1] if a greater number is desired."));
    }

    public void testCreateProcessorWithInvalidInferenceConfig() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client,
            clusterService,
            Settings.EMPTY,
            ingestService);

        Map<String, Object> config = new HashMap<>() {{
            put(InferenceProcessor.FIELD_MAPPINGS, Collections.emptyMap());
            put(InferenceProcessor.MODEL_ID, "my_model");
            put(InferenceProcessor.TARGET_FIELD, "result");
            put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap("unknown_type", Collections.emptyMap()));
        }};

        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", config));
        assertThat(ex.getMessage(),
            equalTo("unrecognized inference configuration type [unknown_type]. Supported types [classification, regression]"));

        Map<String, Object> config2 = new HashMap<>() {{
            put(InferenceProcessor.FIELD_MAPPINGS, Collections.emptyMap());
            put(InferenceProcessor.MODEL_ID, "my_model");
            put(InferenceProcessor.TARGET_FIELD, "result");
            put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap("regression", "boom"));
        }};
        ex = expectThrows(ElasticsearchStatusException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", config2));
        assertThat(ex.getMessage(),
            equalTo("inference_config must be an object with one inference type mapped to an object."));

        Map<String, Object> config3 = new HashMap<>() {{
            put(InferenceProcessor.FIELD_MAPPINGS, Collections.emptyMap());
            put(InferenceProcessor.MODEL_ID, "my_model");
            put(InferenceProcessor.TARGET_FIELD, "result");
            put(InferenceProcessor.INFERENCE_CONFIG, Collections.emptyMap());
        }};
        ex = expectThrows(ElasticsearchStatusException.class,
            () -> processorFactory.create(Collections.emptyMap(), "my_inference_processor", config3));
        assertThat(ex.getMessage(),
            equalTo("inference_config must be an object with one inference type mapped to an object."));
    }

    public void testCreateProcessorWithTooOldMinNodeVersion() throws IOException {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client,
            clusterService,
            Settings.EMPTY,
            ingestService);
        processorFactory.accept(builderClusterStateWithModelReferences(Version.V_7_5_0, "model1"));

        Map<String, Object> regression = new HashMap<>() {{
            put(InferenceProcessor.FIELD_MAPPINGS, Collections.emptyMap());
            put(InferenceProcessor.MODEL_ID, "my_model");
            put(InferenceProcessor.TARGET_FIELD, "result");
            put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap(RegressionConfig.NAME, Collections.emptyMap()));
        }};

        try {
            processorFactory.create(Collections.emptyMap(), "my_inference_processor", regression);
            fail("Should not have successfully created");
        } catch (ElasticsearchException ex) {
            assertThat(ex.getMessage(),
                equalTo("Configuration [regression] requires minimum node version [7.6.0] (current minimum node version [7.5.0]"));
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        Map<String, Object> classification = new HashMap<>() {{
            put(InferenceProcessor.FIELD_MAPPINGS, Collections.emptyMap());
            put(InferenceProcessor.MODEL_ID, "my_model");
            put(InferenceProcessor.TARGET_FIELD, "result");
            put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap(ClassificationConfig.NAME,
                Collections.singletonMap(ClassificationConfig.NUM_TOP_CLASSES.getPreferredName(), 1)));
        }};

        try {
            processorFactory.create(Collections.emptyMap(), "my_inference_processor", classification);
            fail("Should not have successfully created");
        } catch (ElasticsearchException ex) {
            assertThat(ex.getMessage(),
                equalTo("Configuration [classification] requires minimum node version [7.6.0] (current minimum node version [7.5.0]"));
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testCreateProcessor() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client,
            clusterService,
            Settings.EMPTY,
            ingestService);

        Map<String, Object> regression = new HashMap<>() {{
            put(InferenceProcessor.FIELD_MAPPINGS, Collections.emptyMap());
            put(InferenceProcessor.MODEL_ID, "my_model");
            put(InferenceProcessor.TARGET_FIELD, "result");
            put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap(RegressionConfig.NAME, Collections.emptyMap()));
        }};

        try {
            processorFactory.create(Collections.emptyMap(), "my_inference_processor", regression);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        Map<String, Object> classification = new HashMap<>() {{
            put(InferenceProcessor.FIELD_MAPPINGS, Collections.emptyMap());
            put(InferenceProcessor.MODEL_ID, "my_model");
            put(InferenceProcessor.TARGET_FIELD, "result");
            put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap(ClassificationConfig.NAME,
                Collections.singletonMap(ClassificationConfig.NUM_TOP_CLASSES.getPreferredName(), 1)));
        }};

        try {
            processorFactory.create(Collections.emptyMap(), "my_inference_processor", classification);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testCreateProcessorWithDuplicateFields() {
        InferenceProcessor.Factory processorFactory = new InferenceProcessor.Factory(client,
            clusterService,
            Settings.EMPTY,
            ingestService);

        Map<String, Object> regression = new HashMap<>() {{
            put(InferenceProcessor.FIELD_MAPPINGS, Collections.emptyMap());
            put(InferenceProcessor.MODEL_ID, "my_model");
            put(InferenceProcessor.TARGET_FIELD, "ml");
            put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap(RegressionConfig.NAME,
                Collections.singletonMap(RegressionConfig.RESULTS_FIELD.getPreferredName(), "warning")));
        }};

        try {
            processorFactory.create(Collections.emptyMap(), "my_inference_processor", regression);
            fail("should not have succeeded creating with duplicate fields");
        } catch (Exception ex) {
            assertThat(ex.getMessage(), equalTo("Cannot create processor as configured. " +
                "More than one field is configured as [warning]"));
        }
    }

    private static ClusterState buildClusterState(MetaData metaData) {
       return ClusterState.builder(new ClusterName("_name")).metaData(metaData).build();
    }

    private static ClusterState buildClusterStateWithModelReferences(String... modelId) throws IOException {
        return builderClusterStateWithModelReferences(Version.CURRENT, modelId);
    }

    private static ClusterState builderClusterStateWithModelReferences(Version minNodeVersion, String... modelId) throws IOException {
        Map<String, PipelineConfiguration> configurations = new HashMap<>(modelId.length);
        for (String id : modelId) {
            configurations.put("pipeline_with_model_" + id, newConfigurationWithInferenceProcessor(id));
        }
        IngestMetadata ingestMetadata = new IngestMetadata(configurations);

        return ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .nodes(DiscoveryNodes.builder()
                .add(new DiscoveryNode("min_node",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    minNodeVersion))
                .add(new DiscoveryNode("current_node",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                    Version.CURRENT))
                .localNodeId("_node_id")
                .masterNodeId("_node_id"))
            .build();
    }

    private static PipelineConfiguration newConfigurationWithInferenceProcessor(String modelId) throws IOException {
        try(XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(Collections.singletonMap("processors",
            Collections.singletonList(
                Collections.singletonMap(InferenceProcessor.TYPE,
                    new HashMap<>() {{
                        put(InferenceProcessor.MODEL_ID, modelId);
                        put(InferenceProcessor.INFERENCE_CONFIG, Collections.singletonMap(RegressionConfig.NAME, Collections.emptyMap()));
                        put(InferenceProcessor.TARGET_FIELD, "new_field");
                        put(InferenceProcessor.FIELD_MAPPINGS, Collections.singletonMap("source", "dest"));
                    }}))))) {
            return new PipelineConfiguration("pipeline_with_model_" + modelId, BytesReference.bytes(xContentBuilder), XContentType.JSON);
        }
    }

}
