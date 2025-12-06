/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RequiredField;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * {@link DestinationIndex} class encapsulates logic for creating destination index based on source index metadata.
 */
public final class DestinationIndex {

    private static final Logger logger = LogManager.getLogger(DestinationIndex.class);

    public static final String INCREMENTAL_ID = "ml__incremental_id";

    /**
     * The field that indicates whether a doc was used for training or not
     */
    public static final String IS_TRAINING = "is_training";

    // Metadata fields
    static final String CREATION_DATE_MILLIS = "creation_date_in_millis";
    static final String VERSION = "version";
    static final String CREATED = "created";
    static final String CREATED_BY = "created_by";
    static final String ANALYTICS = "analytics";

    private static final String PROPERTIES = "properties";
    private static final String META = "_meta";
    private static final String RUNTIME = "runtime";

    private static final String DFA_CREATOR = "data-frame-analytics";

    /**
     * We only preserve the most important settings.
     * If the user needs other settings on the destination index they
     * should create the destination index before starting the analytics.
     */
    private static final String[] PRESERVED_SETTINGS = new String[] {
        "index.number_of_shards",
        "index.number_of_replicas",
        "index.analysis.*",
        "index.similarity.*",
        "index.mapping.*" };

    /**
     * This is the minimum compatible version of the destination index we can currently work with.
     * If the results mappings change in a way existing destination indices will fail to index
     * the results, this should be bumped accordingly.
     */
    public static final MlConfigVersion MIN_COMPATIBLE_VERSION =
        StartDataFrameAnalyticsAction.TaskParams.VERSION_DESTINATION_INDEX_MAPPINGS_CHANGED;

    private DestinationIndex() {}

    /**
     * Creates destination index based on source index metadata.
     */
    public static void createDestinationIndex(
        Client client,
        Clock clock,
        DataFrameAnalyticsConfig analyticsConfig,
        String[] destIndexAllowedSettings,
        ActionListener<CreateIndexResponse> listener
    ) {
        prepareCreateIndexRequest(
            client,
            clock,
            analyticsConfig,
            destIndexAllowedSettings,
            listener.delegateFailureAndWrap(
                (l, createIndexRequest) -> ClientHelper.executeWithHeadersAsync(
                    analyticsConfig.getHeaders(),
                    ClientHelper.ML_ORIGIN,
                    client,
                    TransportCreateIndexAction.TYPE,
                    createIndexRequest,
                    l
                )
            )
        );
    }

    private static void prepareCreateIndexRequest(
        Client client,
        Clock clock,
        DataFrameAnalyticsConfig config,
        String[] destIndexAllowedSettings,
        ActionListener<CreateIndexRequest> listener
    ) {
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest(MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT)
            .indices(config.getSource().getIndex())
            .indicesOptions(IndicesOptions.lenientExpandOpen())
            .names(PRESERVED_SETTINGS);
        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(),
            ML_ORIGIN,
            client,
            GetSettingsAction.INSTANCE,
            getSettingsRequest,
            listener.delegateFailureAndWrap((delegate, settingsResponse) -> {
                final Settings settings = settings(settingsResponse, destIndexAllowedSettings);
                MappingsMerger.mergeMappings(
                    client,
                    MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                    config.getHeaders(),
                    config.getSource(),
                    delegate.delegateFailureAndWrap(
                        (l, mappings) -> getFieldCapsForRequiredFields(
                            client,
                            config,
                            l.delegateFailureAndWrap(
                                (ll, fieldCapabilitiesResponse) -> ll.onResponse(
                                    createIndexRequest(clock, config, settings, mappings, fieldCapabilitiesResponse)
                                )
                            )
                        )
                    )
                );
            })
        );
    }

    private static void getFieldCapsForRequiredFields(
        Client client,
        DataFrameAnalyticsConfig config,
        ActionListener<FieldCapabilitiesResponse> listener
    ) {
        List<RequiredField> requiredFields = config.getAnalysis().getRequiredFields();
        if (requiredFields.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().indices(config.getSource().getIndex())
            .fields(requiredFields.stream().map(RequiredField::getName).toArray(String[]::new))
            .runtimeFields(config.getSource().getRuntimeMappings());
        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(),
            ML_ORIGIN,
            client,
            TransportFieldCapabilitiesAction.TYPE,
            fieldCapabilitiesRequest,
            listener
        );
    }

    private static CreateIndexRequest createIndexRequest(
        Clock clock,
        DataFrameAnalyticsConfig config,
        Settings settings,
        MappingMetadata mappings,
        FieldCapabilitiesResponse fieldCapabilitiesResponse
    ) {
        String destinationIndex = config.getDest().getIndex();
        Map<String, Object> mappingsAsMap = mappings.sourceAsMap();
        Map<String, Object> properties = getOrPutDefault(mappingsAsMap, PROPERTIES, HashMap::new);
        checkResultsFieldIsNotPresentInProperties(config, properties);
        properties.putAll(createAdditionalMappings(config, fieldCapabilitiesResponse));
        Map<String, Object> metadata = getOrPutDefault(mappingsAsMap, META, HashMap::new);
        metadata.putAll(createMetadata(config.getId(), clock, MlConfigVersion.CURRENT));
        if (config.getSource().getRuntimeMappings().isEmpty() == false) {
            Map<String, Object> runtimeMappings = getOrPutDefault(mappingsAsMap, RUNTIME, HashMap::new);
            runtimeMappings.putAll(config.getSource().getRuntimeMappings());
        }
        return new CreateIndexRequest(destinationIndex, settings).mapping(mappingsAsMap);
    }

    private static Settings settings(GetSettingsResponse settingsResponse, String[] destIndexAllowedSettings) {
        Settings.Builder settingsBuilder = Settings.builder();

        for (String key : destIndexAllowedSettings) {
            Long value = findMaxSettingValue(settingsResponse, key);
            if (value != null) {
                settingsBuilder.put(key, value);
            }
        }

        Map<String, Tuple<String, Settings>> mergedSettings = new HashMap<>();

        mergeSimilaritySettings(settingsResponse, mergedSettings);
        mergeAnalysisSettings(settingsResponse, mergedSettings);

        for (String settingsKey : Arrays.asList(
            IndexModule.SIMILARITY_SETTINGS_PREFIX,
            AnalysisRegistry.INDEX_ANALYSIS_FILTER,
            AnalysisRegistry.INDEX_ANALYSIS_ANALYZER
        )) {
            for (Map.Entry<String, Tuple<String, Settings>> mergedSetting : mergedSettings.entrySet()) {
                String index = mergedSetting.getValue().v1();
                Set<String> settingsKeys = settingsResponse.getIndexToSettings().get(index).getAsSettings(settingsKey).keySet();
                for (String key : settingsKeys) {
                    settingsBuilder = settingsBuilder.copy(settingsKey + "." + key, settingsResponse.getIndexToSettings().get(index));
                }
            }
        }
        return settingsBuilder.build();
    }

    private static void mergeSimilaritySettings(GetSettingsResponse settingsResponse, Map<String, Tuple<String, Settings>> mergedSettings) {
        String settingsKey = IndexModule.SIMILARITY_SETTINGS_PREFIX;

        for (Map.Entry<String, Settings> settingsEntry : settingsResponse.getIndexToSettings().entrySet()) {

            Settings settings = settingsEntry.getValue().getAsSettings(settingsKey);
            if (settings.isEmpty()) {
                continue;
            }

            mergeSettings(settingsKey, settingsEntry.getKey(), settings, mergedSettings);
        }
    }

    private static void mergeAnalysisSettings(GetSettingsResponse settingsResponse, Map<String, Tuple<String, Settings>> mergedSettings) {
        for (String settingsKey : Arrays.asList(AnalysisRegistry.INDEX_ANALYSIS_FILTER, AnalysisRegistry.INDEX_ANALYSIS_ANALYZER)) {
            for (Map.Entry<String, Settings> settingsEntry : settingsResponse.getIndexToSettings().entrySet()) {

                Settings settings = settingsEntry.getValue().getAsSettings(settingsKey);
                if (settings.isEmpty()) {
                    continue;
                }

                for (String name : settings.names()) {
                    Settings setting = settings.getAsSettings(name);
                    String fullName = settingsKey + "." + name;

                    mergeSettings(fullName, settingsEntry.getKey(), setting, mergedSettings);
                }
            }
        }
    }

    private static void mergeSettings(String key, String index, Settings setting, Map<String, Tuple<String, Settings>> mergedSettings) {
        if (mergedSettings.containsKey(key) == false) {
            mergedSettings.put(key, new Tuple<>(index, setting));
        } else {
            Settings mergedSetting = mergedSettings.get(key).v2();
            if (mergedSetting.equals(setting) == false) {
                throw ExceptionsHelper.badRequestException(
                    "cannot merge settings because of differences for "
                        + key
                        + "; specified as [{}] in index [{}]; "
                        + "specified as [{}] in index [{}]",
                    mergedSettings.get(key).v2(),
                    mergedSettings.get(key).v1(),
                    setting.toString(),
                    index
                );
            }
        }
    }

    @Nullable
    private static Long findMaxSettingValue(GetSettingsResponse settingsResponse, String settingKey) {
        Long maxValue = null;
        for (Settings settings : settingsResponse.getIndexToSettings().values()) {
            Long indexValue = settings.getAsLong(settingKey, null);
            if (indexValue != null) {
                maxValue = maxValue == null ? indexValue : Math.max(indexValue, maxValue);
            }
        }
        return maxValue;
    }

    private static Map<String, Object> createAdditionalMappings(
        DataFrameAnalyticsConfig config,
        FieldCapabilitiesResponse fieldCapabilitiesResponse
    ) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(INCREMENTAL_ID, Map.of("type", NumberFieldMapper.NumberType.LONG.typeName()));
        properties.putAll(config.getAnalysis().getResultMappings(config.getDest().getResultsField(), fieldCapabilitiesResponse));
        return properties;
    }

    // Visible for testing
    static Map<String, Object> createMetadata(String analyticsId, Clock clock, MlConfigVersion version) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(CREATION_DATE_MILLIS, clock.millis());
        metadata.put(CREATED_BY, DFA_CREATOR);
        metadata.put(VERSION, Map.of(CREATED, version.toString()));
        metadata.put(ANALYTICS, analyticsId);
        return metadata;
    }

    @SuppressWarnings("unchecked")
    private static <K, V> V getOrPutDefault(Map<K, Object> map, K key, Supplier<V> valueSupplier) {
        V value = (V) map.get(key);
        if (value == null) {
            value = valueSupplier.get();
            map.put(key, value);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public static void updateMappingsToDestIndex(
        Client client,
        DataFrameAnalyticsConfig config,
        GetIndexResponse getIndexResponse,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // We have validated the destination index should match a single index
        assert getIndexResponse.indices().length == 1;

        // Fetch mappings from destination index
        Map<String, Object> destMappingsAsMap = getIndexResponse.mappings().values().iterator().next().sourceAsMap();
        Map<String, Object> destPropertiesAsMap = (Map<String, Object>) destMappingsAsMap.getOrDefault(PROPERTIES, Collections.emptyMap());

        // Verify that the results field does not exist in the dest index
        checkResultsFieldIsNotPresentInProperties(config, destPropertiesAsMap);

        getFieldCapsForRequiredFields(client, config, listener.delegateFailureAndWrap((delegate, fieldCapabilitiesResponse) -> {
            Map<String, Object> addedMappings = new HashMap<>();

            // Determine mappings to be added to the destination index
            addedMappings.put(PROPERTIES, createAdditionalMappings(config, fieldCapabilitiesResponse));

            // Also add runtime mappings
            if (config.getSource().getRuntimeMappings().isEmpty() == false) {
                addedMappings.put(RUNTIME, config.getSource().getRuntimeMappings());
            }

            // Add the mappings to the destination index
            PutMappingRequest putMappingRequest = new PutMappingRequest(getIndexResponse.indices()).source(addedMappings);
            ClientHelper.executeWithHeadersAsync(
                config.getHeaders(),
                ML_ORIGIN,
                client,
                TransportPutMappingAction.TYPE,
                putMappingRequest,
                delegate
            );
        }));
    }

    private static void checkResultsFieldIsNotPresentInProperties(DataFrameAnalyticsConfig config, Map<String, Object> properties) {
        String resultsField = config.getDest().getResultsField();
        if (properties.containsKey(resultsField)) {
            throw ExceptionsHelper.badRequestException(
                "A field that matches the {}.{} [{}] already exists; please set a different {}",
                DataFrameAnalyticsConfig.DEST.getPreferredName(),
                DataFrameAnalyticsDest.RESULTS_FIELD.getPreferredName(),
                resultsField,
                DataFrameAnalyticsDest.RESULTS_FIELD.getPreferredName()
            );
        }
    }

    @SuppressWarnings("unchecked")
    public static Metadata readMetadata(String jobId, MappingMetadata mappingMetadata) {
        Map<String, Object> mappings = mappingMetadata.getSourceAsMap();
        Map<String, Object> meta = (Map<String, Object>) mappings.get(META);
        if ((meta == null) || (DFA_CREATOR.equals(meta.get(CREATED_BY)) == false)) {
            return new NoMetadata();
        }
        return new DestMetadata(getVersion(jobId, meta));
    }

    @SuppressWarnings("unchecked")
    private static MlConfigVersion getVersion(String jobId, Map<String, Object> meta) {
        try {
            Map<String, Object> version = (Map<String, Object>) meta.get(VERSION);
            String createdVersionString = (String) version.get(CREATED);
            return MlConfigVersion.fromString(createdVersionString);
        } catch (Exception e) {
            logger.error(() -> "[" + jobId + "] Could not retrieve destination index version", e);
            return null;
        }
    }

    public interface Metadata {

        boolean hasMetadata();

        boolean isCompatible();

        String getVersion();
    }

    private static class NoMetadata implements Metadata {

        @Override
        public boolean hasMetadata() {
            return false;
        }

        @Override
        public boolean isCompatible() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getVersion() {
            throw new UnsupportedOperationException();
        }
    }

    private static class DestMetadata implements Metadata {

        private final MlConfigVersion version;

        private DestMetadata(MlConfigVersion version) {
            this.version = version;
        }

        @Override
        public boolean hasMetadata() {
            return true;
        }

        @Override
        public boolean isCompatible() {
            return version == null ? false : version.onOrAfter(MIN_COMPATIBLE_VERSION);
        }

        @Override
        public String getVersion() {
            return version == null ? "unknown" : version.toString();
        }
    }
}
