/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettings;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public final class TransformIndex {
    private static final Logger logger = LogManager.getLogger(TransformIndex.class);

    private static final String PROPERTIES = "properties";
    private static final String META = "_meta";

    private TransformIndex() {}

    /**
     * Checks whether the given index was created automatically by the transform rather than explicitly by the user.
     *
     * @param client ES client
     * @param destIndex Destination index name/pattern
     * @param listener Listener to be called after the check is done.
     *                 Returns {@code true} if the given index was created by the transform and {@code false} otherwise.
     */
    public static void isDestinationIndexCreatedByTransform(Client client, String destIndex, ActionListener<Boolean> listener) {
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(destIndex)
            // We only need mappings, more specifically its "_meta" part
            .features(GetIndexRequest.Feature.MAPPINGS);
        executeAsyncWithOrigin(client, TRANSFORM_ORIGIN, GetIndexAction.INSTANCE, getIndexRequest, ActionListener.wrap(getIndexResponse -> {
            ImmutableOpenMap<String, MappingMetadata> indicesMappings = getIndexResponse.mappings();
            if (indicesMappings.containsKey(destIndex) == false) {
                listener.onResponse(false);
                return;
            }
            Map<String, Object> indexMapping = indicesMappings.get(destIndex).getSourceAsMap();
            if (indexMapping.containsKey(META) == false) {
                listener.onResponse(false);
                return;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> indexMappingMetadata = (Map<String, Object>) indexMapping.get(META);
            if (indexMappingMetadata.containsKey(TransformField.CREATED_BY) == false) {
                listener.onResponse(false);
                return;
            }
            String createdBy = (String) indexMappingMetadata.get(TransformField.CREATED_BY);
            if (TransformField.TRANSFORM_SIGNATURE.equals(createdBy) == false) {
                listener.onResponse(false);
                return;
            }
            listener.onResponse(true);
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                // Missing index is ok, we should report that it was not created by transform (because it doesn't exist)
                listener.onResponse(false);
                return;
            }
            listener.onFailure(e);
        }));
    }

    public static void createDestinationIndex(
        Client client,
        TransformConfig transformConfig,
        TransformDestIndexSettings destIndexSettings,
        ActionListener<Boolean> listener
    ) {
        CreateIndexRequest request = new CreateIndexRequest(transformConfig.getDestination().getIndex());

        request.settings(destIndexSettings.getSettings());
        request.mapping(destIndexSettings.getMappings());
        for (Alias alias : destIndexSettings.getAliases()) {
            request.alias(alias);
        }

        client.execute(
            CreateIndexAction.INSTANCE,
            request,
            ActionListener.wrap(createIndexResponse -> { listener.onResponse(true); }, e -> {
                String message = TransformMessages.getMessage(
                    TransformMessages.FAILED_TO_CREATE_DESTINATION_INDEX,
                    transformConfig.getDestination().getIndex(),
                    transformConfig.getId()
                );
                logger.error(message);
                listener.onFailure(new RuntimeException(message, e));
            })
        );
    }

    public static TransformDestIndexSettings createTransformDestIndexSettings(Map<String, String> mappings, String id, Clock clock) {
        Map<String, Object> indexMappings = new HashMap<>();
        indexMappings.put(PROPERTIES, createMappingsFromStringMap(mappings));
        indexMappings.put(META, createMetadata(id, clock));

        Settings settings = createSettings();

        // transform does not create aliases, however the user might customize this in future
        Set<Alias> aliases = null;
        return new TransformDestIndexSettings(indexMappings, settings, aliases);
    }

    /*
     * Return meta data that stores some useful information about the transform index, stored as "_meta":
     *
     * {
     *   "created_by" : "transform",
     *   "_transform" : {
     *     "transform" : "id",
     *     "version" : {
     *       "created" : "8.0.0"
     *     },
     *     "creation_date_in_millis" : 1584025695202
     *   }
     * }
     */
    private static Map<String, Object> createMetadata(String id, Clock clock) {

        Map<String, Object> metadata = new HashMap<>();
        metadata.put(TransformField.CREATED_BY, TransformField.TRANSFORM_SIGNATURE);

        Map<String, Object> transformMetadata = new HashMap<>();
        transformMetadata.put(TransformField.CREATION_DATE_MILLIS, clock.millis());
        transformMetadata.put(TransformField.VERSION.getPreferredName(), Map.of(TransformField.CREATED, Version.CURRENT.toString()));
        transformMetadata.put(TransformField.TRANSFORM, id);

        metadata.put(TransformField.META_FIELDNAME, transformMetadata);
        return metadata;
    }

    /**
     * creates generated index settings, hardcoded at the moment, in future this might be customizable or generation could
     * be based on source settings.
     */
    private static Settings createSettings() {
        return Settings.builder() // <1>
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    /**
     * This takes the a {@code Map<String, String>} of the type "fieldname: fieldtype" and transforms it into the
     * typical mapping format.
     *
     * Example:
     *
     * input:
     * {"field1.subField1": "long", "field2": "keyword"}
     *
     * output:
     * {
     *   "field1.subField1": {
     *     "type": "long"
     *   },
     *   "field2": {
     *     "type": "keyword"
     *   }
     * }
     * @param mappings A Map of the form {"fieldName": "fieldType"}
     */
    static Map<String, Object> createMappingsFromStringMap(Map<String, String> mappings) {
        return mappings.entrySet().stream().collect(toMap(e -> e.getKey(), e -> singletonMap("type", e.getValue())));
    }
}
