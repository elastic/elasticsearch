/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettings;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.Entry.comparingByKey;

public final class TransformIndex {
    private static final Logger logger = LogManager.getLogger(TransformIndex.class);

    /**
     * The list of object types used in the mappings.
     * We include {@code null} as an alternative for "object", which is the default.
     */
    private static final Set<String> OBJECT_TYPES =
        new HashSet<>(Arrays.asList(null, ObjectMapper.CONTENT_TYPE, ObjectMapper.NESTED_CONTENT_TYPE));
    private static final String PROPERTIES = "properties";
    private static final String FIELDS = "fields";
    private static final String META = "_meta";

    private TransformIndex() {}

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
        transformMetadata.put(TransformField.VERSION.getPreferredName(), Map.of(TransformField.CREATED, Version.CURRENT));
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
        List<Map.Entry<String, String>> sortedMappingsEntries = new ArrayList<>(mappings.entrySet());
        // We sort the entry list to make sure that for each (parent, parent.child) pair, parent entry will be processed before child entry.
        sortedMappingsEntries.sort(comparingByKey());
        Map<String, Object> fieldMappings = new HashMap<>();
        for (Map.Entry<String, String> entry : sortedMappingsEntries) {
            String[] parts = Strings.tokenizeToStringArray(entry.getKey(), ".");
            String type = entry.getValue();
            Map<String, Object> current = fieldMappings;
            current = diveInto(current, parts[0]);
            for (int j = 1; j < parts.length; ++j) {
                // Here we decide whether a dot ('.') means inner object or a multi-field.
                current = diveInto(current, OBJECT_TYPES.contains(current.get("type")) ? PROPERTIES : FIELDS);
                current = diveInto(current, parts[j]);
            }
            current.put("type", type);
        }
        return fieldMappings;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> diveInto(Map<String, Object> map, String key) {
        return (Map<String, Object>) map.computeIfAbsent(key, k -> new HashMap<>());
    }
}
