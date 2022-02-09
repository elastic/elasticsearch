/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.ingest.IngestDocument.INGEST_KEY;
import static org.elasticsearch.ingest.Pipeline.PROCESSORS_KEY;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.inference.results.InferenceResults.MODEL_ID_RESULTS_FIELD;

public class InferenceProcessor extends AbstractProcessor {

    // How many total inference processors are allowed to be used in the cluster.
    public static final Setting<Integer> MAX_INFERENCE_PROCESSORS = Setting.intSetting(
        "xpack.ml.max_inference_processors",
        50,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final String TYPE = "inference";
    public static final String INFERENCE_CONFIG = "inference_config";
    public static final String TARGET_FIELD = "target_field";
    public static final String FIELD_MAPPINGS = "field_mappings";
    public static final String FIELD_MAP = "field_map";
    private static final String DEFAULT_TARGET_FIELD = "ml.inference";

    private final Client client;
    private final String modelId;

    private final String targetField;
    private final InferenceConfigUpdate inferenceConfig;
    private final Map<String, String> fieldMap;
    private final InferenceAuditor auditor;
    private volatile boolean previouslyLicensed;
    private final AtomicBoolean shouldAudit = new AtomicBoolean(true);

    public InferenceProcessor(
        Client client,
        InferenceAuditor auditor,
        String tag,
        String description,
        String targetField,
        String modelId,
        InferenceConfigUpdate inferenceConfig,
        Map<String, String> fieldMap
    ) {
        super(tag, description);
        this.client = ExceptionsHelper.requireNonNull(client, "client");
        this.targetField = ExceptionsHelper.requireNonNull(targetField, TARGET_FIELD);
        this.auditor = ExceptionsHelper.requireNonNull(auditor, "auditor");
        this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID_RESULTS_FIELD);
        this.inferenceConfig = ExceptionsHelper.requireNonNull(inferenceConfig, INFERENCE_CONFIG);
        this.fieldMap = ExceptionsHelper.requireNonNull(fieldMap, FIELD_MAP);
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            InternalInferModelAction.INSTANCE,
            this.buildRequest(ingestDocument),
            ActionListener.wrap(r -> handleResponse(r, ingestDocument, handler), e -> handler.accept(ingestDocument, e))
        );
    }

    void handleResponse(
        InternalInferModelAction.Response response,
        IngestDocument ingestDocument,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        if (previouslyLicensed == false) {
            previouslyLicensed = true;
        }
        if (response.isLicensed() == false) {
            auditWarningAboutLicenseIfNecessary();
        }
        try {
            mutateDocument(response, ingestDocument);
            handler.accept(ingestDocument, null);
        } catch (ElasticsearchException ex) {
            handler.accept(ingestDocument, ex);
        }
    }

    InternalInferModelAction.Request buildRequest(IngestDocument ingestDocument) {
        Map<String, Object> fields = new HashMap<>(ingestDocument.getSourceAndMetadata());
        // Add ingestMetadata as previous processors might have added metadata from which we are predicting (see: foreach processor)
        if (ingestDocument.getIngestMetadata().isEmpty() == false) {
            fields.put(INGEST_KEY, ingestDocument.getIngestMetadata());
        }
        LocalModel.mapFieldsIfNecessary(fields, fieldMap);
        return new InternalInferModelAction.Request(modelId, fields, inferenceConfig, previouslyLicensed);
    }

    void auditWarningAboutLicenseIfNecessary() {
        if (shouldAudit.compareAndSet(true, false)) {
            auditor.warning(
                modelId,
                "This cluster is no longer licensed to use this model in the inference ingest processor. "
                    + "Please update your license information."
            );
        }
    }

    void mutateDocument(InternalInferModelAction.Response response, IngestDocument ingestDocument) {
        if (response.getInferenceResults().isEmpty()) {
            throw new ElasticsearchStatusException("Unexpected empty inference response", RestStatus.INTERNAL_SERVER_ERROR);
        }
        assert response.getInferenceResults().size() == 1;
        InferenceResults.writeResult(
            response.getInferenceResults().get(0),
            ingestDocument,
            targetField,
            response.getModelId() != null ? response.getModelId() : modelId
        );
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        throw new UnsupportedOperationException("should never be called");
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory, Consumer<ClusterState> {

        private static final String FOREACH_PROCESSOR_NAME = "foreach";
        // Any more than 10 nestings of processors, we stop searching for inference processor definitions
        private static final int MAX_INFERENCE_PROCESSOR_SEARCH_RECURSIONS = 10;
        private static final Logger logger = LogManager.getLogger(Factory.class);

        private final Client client;
        private final InferenceAuditor auditor;
        private volatile int currentInferenceProcessors;
        private volatile int maxIngestProcessors;
        private volatile Version minNodeVersion = Version.CURRENT;

        public Factory(Client client, ClusterService clusterService, Settings settings) {
            this.client = client;
            this.maxIngestProcessors = MAX_INFERENCE_PROCESSORS.get(settings);
            this.auditor = new InferenceAuditor(client, clusterService);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_INFERENCE_PROCESSORS, this::setMaxIngestProcessors);
        }

        @Override
        public void accept(ClusterState state) {
            minNodeVersion = state.nodes().getMinNodeVersion();
            currentInferenceProcessors = countNumberInferenceProcessors(state);
        }

        public static int countNumberInferenceProcessors(ClusterState state) {
            Metadata metadata = state.getMetadata();
            if (metadata == null) {
                return 0;
            }
            IngestMetadata ingestMetadata = metadata.custom(IngestMetadata.TYPE);
            if (ingestMetadata == null) {
                return 0;
            }

            int count = 0;
            for (PipelineConfiguration configuration : ingestMetadata.getPipelines().values()) {
                Map<String, Object> configMap = configuration.getConfigAsMap();
                try {
                    List<Map<String, Object>> processorConfigs = ConfigurationUtils.readList(null, null, configMap, PROCESSORS_KEY);
                    for (Map<String, Object> processorConfigWithKey : processorConfigs) {
                        for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                            count += numInferenceProcessors(entry.getKey(), entry.getValue());
                        }
                    }
                    // We cannot throw any exception here. It might break other pipelines.
                } catch (Exception ex) {
                    logger.debug(
                        () -> new ParameterizedMessage("failed gathering processors for pipeline [{}]", configuration.getId()),
                        ex
                    );
                }
            }
            return count;
        }

        @SuppressWarnings("unchecked")
        static int numInferenceProcessors(String processorType, Object processorDefinition) {
            return numInferenceProcessors(processorType, (Map<String, Object>) processorDefinition, 0);
        }

        @SuppressWarnings("unchecked")
        static int numInferenceProcessors(String processorType, Map<String, Object> processorDefinition, int level) {
            int count = 0;
            // arbitrary, but we must limit this somehow
            if (level > MAX_INFERENCE_PROCESSOR_SEARCH_RECURSIONS) {
                return count;
            }
            if (processorType == null || processorDefinition == null) {
                return count;
            }
            if (TYPE.equals(processorType)) {
                count++;
            }
            if (FOREACH_PROCESSOR_NAME.equals(processorType)) {
                Map<String, Object> innerProcessor = (Map<String, Object>) processorDefinition.get("processor");
                if (innerProcessor != null) {
                    // a foreach processor should only have a SINGLE nested processor. Iteration is for simplicity's sake.
                    for (Map.Entry<String, Object> innerProcessorWithName : innerProcessor.entrySet()) {
                        count += numInferenceProcessors(
                            innerProcessorWithName.getKey(),
                            (Map<String, Object>) innerProcessorWithName.getValue(),
                            level + 1
                        );
                    }
                }
            }
            if (processorDefinition.containsKey(Pipeline.ON_FAILURE_KEY)) {
                List<Map<String, Object>> onFailureConfigs = ConfigurationUtils.readList(
                    null,
                    null,
                    processorDefinition,
                    Pipeline.ON_FAILURE_KEY
                );
                count += onFailureConfigs.stream()
                    .flatMap(map -> map.entrySet().stream())
                    .mapToInt(entry -> numInferenceProcessors(entry.getKey(), (Map<String, Object>) entry.getValue(), level + 1))
                    .sum();
            }
            return count;
        }

        // Used for testing
        int numInferenceProcessors() {
            return currentInferenceProcessors;
        }

        @Override
        public InferenceProcessor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config
        ) {

            if (this.maxIngestProcessors <= currentInferenceProcessors) {
                throw new ElasticsearchStatusException(
                    "Max number of inference processors reached, total inference processors [{}]. "
                        + "Adjust the setting [{}]: [{}] if a greater number is desired.",
                    RestStatus.CONFLICT,
                    currentInferenceProcessors,
                    MAX_INFERENCE_PROCESSORS.getKey(),
                    maxIngestProcessors
                );
            }

            String modelId = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_ID_RESULTS_FIELD);
            String defaultTargetField = tag == null ? DEFAULT_TARGET_FIELD : DEFAULT_TARGET_FIELD + "." + tag;
            // If multiple inference processors are in the same pipeline, it is wise to tag them
            // The tag will keep default value entries from stepping on each other
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, TARGET_FIELD, defaultTargetField);
            Map<String, String> fieldMap = ConfigurationUtils.readOptionalMap(TYPE, tag, config, FIELD_MAP);
            if (fieldMap == null) {
                fieldMap = ConfigurationUtils.readOptionalMap(TYPE, tag, config, FIELD_MAPPINGS);
                // TODO Remove in 9?.x
                if (fieldMap != null) {
                    LoggingDeprecationHandler.INSTANCE.logRenamedField(null, () -> null, FIELD_MAPPINGS, FIELD_MAP);
                }
            }
            if (fieldMap == null) {
                fieldMap = Collections.emptyMap();
            }

            InferenceConfigUpdate inferenceConfigUpdate;
            Map<String, Object> inferenceConfigMap = ConfigurationUtils.readOptionalMap(TYPE, tag, config, INFERENCE_CONFIG);
            if (inferenceConfigMap == null) {
                if (minNodeVersion.before(EmptyConfigUpdate.minimumSupportedVersion())) {
                    // an inference config is required when the empty update is not supported
                    throw ConfigurationUtils.newConfigurationException(TYPE, tag, INFERENCE_CONFIG, "required property is missing");
                }

                inferenceConfigUpdate = new EmptyConfigUpdate();
            } else {
                inferenceConfigUpdate = inferenceConfigUpdateFromMap(inferenceConfigMap);
            }

            return new InferenceProcessor(client, auditor, tag, description, targetField, modelId, inferenceConfigUpdate, fieldMap);
        }

        // Package private for testing
        void setMaxIngestProcessors(int maxIngestProcessors) {
            logger.trace("updating setting maxIngestProcessors from [{}] to [{}]", this.maxIngestProcessors, maxIngestProcessors);
            this.maxIngestProcessors = maxIngestProcessors;
        }

        InferenceConfigUpdate inferenceConfigUpdateFromMap(Map<String, Object> configMap) {
            ExceptionsHelper.requireNonNull(configMap, INFERENCE_CONFIG);
            if (configMap.size() != 1) {
                throw ExceptionsHelper.badRequestException(
                    "{} must be an object with one inference type mapped to an object.",
                    INFERENCE_CONFIG
                );
            }
            Object value = configMap.values().iterator().next();

            if ((value instanceof Map<?, ?>) == false) {
                throw ExceptionsHelper.badRequestException(
                    "{} must be an object with one inference type mapped to an object.",
                    INFERENCE_CONFIG
                );
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> valueMap = (Map<String, Object>) value;

            if (configMap.containsKey(ClassificationConfig.NAME.getPreferredName())) {
                checkSupportedVersion(ClassificationConfig.EMPTY_PARAMS);
                return ClassificationConfigUpdate.fromMap(valueMap);
            } else if (configMap.containsKey(FillMaskConfig.NAME)) {
                checkNlpSupported(FillMaskConfig.NAME);
                return FillMaskConfigUpdate.fromMap(valueMap);
            } else if (configMap.containsKey(NerConfig.NAME)) {
                checkNlpSupported(NerConfig.NAME);
                return NerConfigUpdate.fromMap(valueMap);
            } else if (configMap.containsKey(PassThroughConfig.NAME)) {
                checkNlpSupported(PassThroughConfig.NAME);
                return PassThroughConfigUpdate.fromMap(valueMap);
            } else if (configMap.containsKey(RegressionConfig.NAME.getPreferredName())) {
                checkSupportedVersion(RegressionConfig.EMPTY_PARAMS);
                return RegressionConfigUpdate.fromMap(valueMap);
            } else if (configMap.containsKey(TextClassificationConfig.NAME)) {
                checkNlpSupported(TextClassificationConfig.NAME);
                return TextClassificationConfigUpdate.fromMap(valueMap);
            } else if (configMap.containsKey(TextEmbeddingConfig.NAME)) {
                checkNlpSupported(TextEmbeddingConfig.NAME);
                return TextEmbeddingConfigUpdate.fromMap(valueMap);
            } else if (configMap.containsKey(ZeroShotClassificationConfig.NAME)) {
                checkNlpSupported(ZeroShotClassificationConfig.NAME);
                return ZeroShotClassificationConfigUpdate.fromMap(valueMap);
            } else {
                throw ExceptionsHelper.badRequestException(
                    "unrecognized inference configuration type {}. Supported types {}",
                    configMap.keySet(),
                    List.of(
                        ClassificationConfig.NAME.getPreferredName(),
                        RegressionConfig.NAME.getPreferredName(),
                        FillMaskConfig.NAME,
                        NerConfig.NAME,
                        PassThroughConfig.NAME,
                        TextClassificationConfig.NAME,
                        TextEmbeddingConfig.NAME,
                        ZeroShotClassificationConfig.NAME
                    )
                );
            }
        }

        void checkNlpSupported(String taskType) {
            if (NlpConfig.MINIMUM_NLP_SUPPORTED_VERSION.after(minNodeVersion)) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(
                        Messages.INFERENCE_CONFIG_NOT_SUPPORTED_ON_VERSION,
                        taskType,
                        NlpConfig.MINIMUM_NLP_SUPPORTED_VERSION,
                        minNodeVersion
                    )
                );
            }
        }

        void checkSupportedVersion(InferenceConfig config) {
            if (config.getMinimalSupportedVersion().after(minNodeVersion)) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(
                        Messages.INFERENCE_CONFIG_NOT_SUPPORTED_ON_VERSION,
                        config.getName(),
                        config.getMinimalSupportedVersion(),
                        minNodeVersion
                    )
                );
            }
        }
    }
}
