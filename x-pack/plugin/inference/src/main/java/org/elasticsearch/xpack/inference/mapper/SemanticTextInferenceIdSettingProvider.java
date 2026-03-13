/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.inference.heuristics.DefaultModelChoiceHeuristics;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_PATH;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_ELSER_ENDPOINT_ID_V2;

/**
 * An {@link IndexSettingProvider} that injects {@link SemanticTextFieldMapper#INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID}
 * into newly created indices. The value is determined by running the model choice heuristics against
 * the currently available inference endpoints in the model registry.
 *
 * <p>This pins the heuristic result at index creation time or time of the first semantic_text field creation
 * to maintain consistency in the model used by all semantic_text fields in an index.
 *
 * <p>The setting is only injected when:
 * <ul>
 *   <li>The setting is not already explicitly provided by the user or an index template.</li>
 *   <li>The heuristics successfully select an inference endpoint.</li>
 * </ul>
 */
public class SemanticTextInferenceIdSettingProvider implements IndexSettingProvider {

    private final Supplier<ModelRegistry> modelRegistrySupplier;
    private final CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory;

    public SemanticTextInferenceIdSettingProvider(
        Supplier<ModelRegistry> modelRegistrySupplier,
        CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory
    ) {
        this.modelRegistrySupplier = modelRegistrySupplier;
        this.mapperServiceFactory = mapperServiceFactory;
    }

    /**
     * If the new index request has any semantic_text field, we apply heuristics to compute the default inference id for
     * semantic_text fields of this index.
     * This allows us to pin the inference endpoint choice as `index.semantic_text.default_inference_id` index setting.
     */
    @Override
    public void provideAdditionalSettings(
        String indexName,
        @Nullable String dataStreamName,
        @Nullable IndexMode templateIndexMode,
        ProjectMetadata projectMetadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings,
        IndexVersion indexVersion,
        Settings.Builder additionalSettings
    ) {
        // do not add `index.semantic_text.default_inference_id` if it's already set in the template or in the create request
        if (indexTemplateAndCreateRequestSettings.hasValue(SemanticTextFieldMapper.INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID.getKey())) {
            return;
        }

        if (hasSemanticTextField(indexName, templateIndexMode, indexTemplateAndCreateRequestSettings, combinedTemplateMappings, indexVersion) == false) {
            return;
        }

        ModelRegistry modelRegistry = modelRegistrySupplier.get();
        if (modelRegistry == null) {
            return;
        }

        Optional<String> inferenceId = DefaultModelChoiceHeuristics.selectSemanticTextInferenceId(modelRegistry);
        inferenceId.ifPresent(id -> additionalSettings.put(SemanticTextFieldMapper.INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID.getKey(), id));
    }

    /*
     * Use a temporary mapper service to generate mappings for the given inputs to determine if the `mappingsSource` generates
     * any semantic_text field.
     */
    private boolean hasSemanticTextField(
        String indexName,
        IndexMode templateIndexMode,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> mappingsSource,
        IndexVersion indexVersion
    ) {
        if (mappingsSource == null || mappingsSource.isEmpty()) {
            return false;
        }

        final int shards = indexTemplateAndCreateRequestSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        final int replicas = indexTemplateAndCreateRequestSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1);
        final var temporarySettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, indexVersion)
            .put(indexTemplateAndCreateRequestSettings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        if (templateIndexMode == IndexMode.TIME_SERIES) {
            temporarySettings.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);
            // Avoid failing because index.routing_path is missing (in case fields are marked as dimension)
            temporarySettings.putList(INDEX_ROUTING_PATH.getKey(), List.of("path"));
        }

        final IndexMetadata temporaryIndexMetadata = IndexMetadata.builder(indexName).settings(temporarySettings).build();
        try (MapperService mapperService = mapperServiceFactory.apply(temporaryIndexMetadata)) {
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappingsSource, MapperService.MergeReason.INDEX_TEMPLATE);
            DocumentMapper documentMapper = mapperService.documentMapper();
            if (documentMapper == null) {
                return false;
            }
            for (Mapper mapper : documentMapper.mappers().fieldMappers()) {
                if (mapper instanceof SemanticTextFieldMapper) {
                    return true;
                }
            }
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * If `index.semantic_text.default_inference_id` is not set and the mapping update request has a semantic_text field,
     * apply heuristics to compute the default inference id and set `index.semantic_text.default_inference_id`
     * which will later be used for generating new mappings and added to index settings.
     */
    @Override
    public void preUpdateMappings(IndexMetadata indexMetadata, CompressedXContent requestSource, Settings.Builder additionalSettings) {
        if (indexMetadata.getSettings().hasValue(SemanticTextFieldMapper.INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID.getKey())) {
            return;
        }
        if (hasSemanticTextField(indexMetadata.getIndex().getName(), indexMetadata.getIndexMode(), indexMetadata.getSettings(), List.of(requestSource),
            SETTING_INDEX_VERSION_CREATED.get(indexMetadata.getSettings())) == false) {
            return;
        }
        ModelRegistry modelRegistry = modelRegistrySupplier.get();
        if (modelRegistry == null) {
            return;
        }

        Optional<String> inferenceId = DefaultModelChoiceHeuristics.selectSemanticTextInferenceId(modelRegistry);
        inferenceId.ifPresent(id -> additionalSettings.put(SemanticTextFieldMapper.INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID.getKey(), id));
    }
}
