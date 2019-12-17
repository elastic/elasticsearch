/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class InferenceProcessor extends AbstractProcessor {

    // How many total inference processors are allowed to be used in the cluster.
    public static final Setting<Integer> MAX_INFERENCE_PROCESSORS = Setting.intSetting("xpack.ml.max_inference_processors",
        50,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope);

    public static final String TYPE = "inference";
    public static final String MODEL_ID = "model_id";
    public static final String INFERENCE_CONFIG = "inference_config";
    public static final String TARGET_FIELD = "target_field";
    public static final String FIELD_MAPPINGS = "field_mappings";
    private static final String DEFAULT_TARGET_FIELD = "ml.inference";

    private final Client client;
    private final String modelId;

    private final String targetField;
    private final InferenceConfig inferenceConfig;
    private final Map<String, String> fieldMapping;
    private final InferenceAuditor auditor;
    private volatile boolean previouslyLicensed;
    private final AtomicBoolean shouldAudit = new AtomicBoolean(true);

    public InferenceProcessor(Client client,
                              InferenceAuditor auditor,
                              String tag,
                              String targetField,
                              String modelId,
                              InferenceConfig inferenceConfig,
                              Map<String, String> fieldMapping) {
        super(tag);
        this.client = ExceptionsHelper.requireNonNull(client, "client");
        this.targetField = ExceptionsHelper.requireNonNull(targetField, TARGET_FIELD);
        this.auditor = ExceptionsHelper.requireNonNull(auditor, "auditor");
        this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        this.inferenceConfig = ExceptionsHelper.requireNonNull(inferenceConfig, INFERENCE_CONFIG);
        this.fieldMapping = ExceptionsHelper.requireNonNull(fieldMapping, FIELD_MAPPINGS);
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            InternalInferModelAction.INSTANCE,
            this.buildRequest(ingestDocument),
            ActionListener.wrap(
                r -> handleResponse(r, ingestDocument, handler),
                e -> handler.accept(ingestDocument, e)
            ));
    }

    void handleResponse(InternalInferModelAction.Response response,
                        IngestDocument ingestDocument,
                        BiConsumer<IngestDocument, Exception> handler) {
        if (previouslyLicensed == false) {
            previouslyLicensed = true;
        }
        if (response.isLicensed() == false) {
            auditWarningAboutLicenseIfNecessary();
        }
        try {
            mutateDocument(response, ingestDocument);
            handler.accept(ingestDocument, null);
        } catch(ElasticsearchException ex) {
            handler.accept(ingestDocument, ex);
        }
    }

    InternalInferModelAction.Request buildRequest(IngestDocument ingestDocument) {
        Map<String, Object> fields = new HashMap<>(ingestDocument.getSourceAndMetadata());
        if (fieldMapping != null) {
            fieldMapping.forEach((src, dest) -> {
                Object srcValue = fields.remove(src);
                if (srcValue != null) {
                    fields.put(dest, srcValue);
                }
            });
        }
        return new InternalInferModelAction.Request(modelId, fields, inferenceConfig, previouslyLicensed);
    }

    void auditWarningAboutLicenseIfNecessary() {
        if (shouldAudit.compareAndSet(true, false)) {
            auditor.warning(
                modelId,
                "This cluster is no longer licensed to use this model in the inference ingest processor. " +
                    "Please update your license information.");
        }
    }

    void mutateDocument(InternalInferModelAction.Response response, IngestDocument ingestDocument) {
        if (response.getInferenceResults().isEmpty()) {
            throw new ElasticsearchStatusException("Unexpected empty inference response", RestStatus.INTERNAL_SERVER_ERROR);
        }
        InferenceResults inferenceResults = response.getInferenceResults().get(0);
        if (inferenceResults instanceof WarningInferenceResults) {
            inferenceResults.writeResult(ingestDocument, this.targetField);
        } else {
            response.getInferenceResults().get(0).writeResult(ingestDocument, this.targetField);
        }
        ingestDocument.setFieldValue(targetField + "." + MODEL_ID, modelId);
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

        private static final Logger logger = LogManager.getLogger(Factory.class);

        private static final Set<String> RESERVED_ML_FIELD_NAMES = new HashSet<>(Arrays.asList(
            WarningInferenceResults.WARNING.getPreferredName(),
            MODEL_ID));

        private final Client client;
        private final IngestService ingestService;
        private final InferenceAuditor auditor;
        private volatile int currentInferenceProcessors;
        private volatile int maxIngestProcessors;
        private volatile Version minNodeVersion = Version.CURRENT;

        public Factory(Client client,
                       ClusterService clusterService,
                       Settings settings,
                       IngestService ingestService) {
            this.client = client;
            this.maxIngestProcessors = MAX_INFERENCE_PROCESSORS.get(settings);
            this.ingestService = ingestService;
            this.auditor = new InferenceAuditor(client, clusterService.getNodeName());
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_INFERENCE_PROCESSORS, this::setMaxIngestProcessors);
        }

        @Override
        public void accept(ClusterState state) {
            minNodeVersion = state.nodes().getMinNodeVersion();
            MetaData metaData = state.getMetaData();
            if (metaData == null) {
                currentInferenceProcessors = 0;
                return;
            }
            IngestMetadata ingestMetadata = metaData.custom(IngestMetadata.TYPE);
            if (ingestMetadata == null) {
                currentInferenceProcessors = 0;
                return;
            }

            int count = 0;
            for (PipelineConfiguration configuration : ingestMetadata.getPipelines().values()) {
                try {
                    Pipeline pipeline = Pipeline.create(configuration.getId(),
                        configuration.getConfigAsMap(),
                        ingestService.getProcessorFactories(),
                        ingestService.getScriptService());
                    count += pipeline.getProcessors().stream().filter(processor -> processor instanceof InferenceProcessor).count();
                } catch (Exception ex) {
                    logger.warn(new ParameterizedMessage("failure parsing pipeline config [{}]", configuration.getId()), ex);
                }
            }
            currentInferenceProcessors = count;
        }

        // Used for testing
        int numInferenceProcessors() {
            return currentInferenceProcessors;
        }

        @Override
        public InferenceProcessor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) {

            if (this.maxIngestProcessors <= currentInferenceProcessors) {
                throw new ElasticsearchStatusException("Max number of inference processors reached, total inference processors [{}]. " +
                    "Adjust the setting [{}]: [{}] if a greater number is desired.",
                    RestStatus.CONFLICT,
                    currentInferenceProcessors,
                    MAX_INFERENCE_PROCESSORS.getKey(),
                    maxIngestProcessors);
            }

            String modelId = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_ID);
            String defaultTargetField = tag == null ? DEFAULT_TARGET_FIELD : DEFAULT_TARGET_FIELD + "." + tag;
            // If multiple inference processors are in the same pipeline, it is wise to tag them
            // The tag will keep default value entries from stepping on each other
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, TARGET_FIELD, defaultTargetField);
            Map<String, String> fieldMapping = ConfigurationUtils.readOptionalMap(TYPE, tag, config, FIELD_MAPPINGS);
            InferenceConfig inferenceConfig = inferenceConfigFromMap(ConfigurationUtils.readMap(TYPE, tag, config, INFERENCE_CONFIG));

            return new InferenceProcessor(client,
                auditor,
                tag,
                targetField,
                modelId,
                inferenceConfig,
                fieldMapping);
        }

        // Package private for testing
        void setMaxIngestProcessors(int maxIngestProcessors) {
            logger.trace("updating setting maxIngestProcessors from [{}] to [{}]", this.maxIngestProcessors, maxIngestProcessors);
            this.maxIngestProcessors = maxIngestProcessors;
        }

        InferenceConfig inferenceConfigFromMap(Map<String, Object> inferenceConfig) {
            ExceptionsHelper.requireNonNull(inferenceConfig, INFERENCE_CONFIG);
            if (inferenceConfig.size() != 1) {
                throw ExceptionsHelper.badRequestException("{} must be an object with one inference type mapped to an object.",
                    INFERENCE_CONFIG);
            }
            Object value = inferenceConfig.values().iterator().next();

            if ((value instanceof Map<?, ?>) == false) {
                throw ExceptionsHelper.badRequestException("{} must be an object with one inference type mapped to an object.",
                    INFERENCE_CONFIG);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> valueMap = (Map<String, Object>)value;

            if (inferenceConfig.containsKey(ClassificationConfig.NAME)) {
                checkSupportedVersion(ClassificationConfig.EMPTY_PARAMS);
                ClassificationConfig config = ClassificationConfig.fromMap(valueMap);
                checkFieldUniqueness(config.getResultsField(), config.getTopClassesResultsField());
                return config;
            } else if (inferenceConfig.containsKey(RegressionConfig.NAME)) {
                checkSupportedVersion(RegressionConfig.EMPTY_PARAMS);
                RegressionConfig config = RegressionConfig.fromMap(valueMap);
                checkFieldUniqueness(config.getResultsField());
                return config;
            } else {
                throw ExceptionsHelper.badRequestException("unrecognized inference configuration type {}. Supported types {}",
                    inferenceConfig.keySet(),
                    Arrays.asList(ClassificationConfig.NAME, RegressionConfig.NAME));
            }
        }

        private static void checkFieldUniqueness(String... fieldNames) {
            Set<String> duplicatedFieldNames = new HashSet<>();
            Set<String> currentFieldNames = new HashSet<>(RESERVED_ML_FIELD_NAMES);
            for(String fieldName : fieldNames) {
                if (currentFieldNames.contains(fieldName)) {
                    duplicatedFieldNames.add(fieldName);
                } else {
                    currentFieldNames.add(fieldName);
                }
            }
            if (duplicatedFieldNames.isEmpty() == false) {
                throw ExceptionsHelper.badRequestException("Cannot create processor as configured." +
                        " More than one field is configured as {}",
                    duplicatedFieldNames);
            }
        }

        void checkSupportedVersion(InferenceConfig config) {
            if (config.getMinimalSupportedVersion().after(minNodeVersion)) {
                throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INFERENCE_CONFIG_NOT_SUPPORTED_ON_VERSION,
                    config.getName(),
                    config.getMinimalSupportedVersion(),
                    minNodeVersion));
            }
        }
    }
}
