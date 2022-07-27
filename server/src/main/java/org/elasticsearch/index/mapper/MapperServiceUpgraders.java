/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Map;

/**
 * Contains a collection of helper methods for index mapping upgrades
 */
public class MapperServiceUpgraders {
    @SuppressWarnings("unchecked")
    private static Map<String, Object> getMappingsForType(Map<String, Object> mappingMap, String type) {
        // Check for MappingMap without type and ignore it
        if (mappingMap.size() != 1) {
            return null;
        }
        // Fetch the mappings for the encoded type
        return (Map<String, Object>) mappingMap.get(type);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static boolean upgradeDateFormatProperties(Map<String, Object> properties, Map<String, Object> parsedProperties) {
        boolean anyChanges = false;

        if (properties != null && parsedProperties != null) {
            for (var property : properties.entrySet()) {
                if (property.getValue()instanceof Map map) {
                    var parsedProperty = parsedProperties.get(property.getKey());
                    if (parsedProperty instanceof Map parsedMap) {
                        Object format = map.get("format");
                        if (format != null) {
                            Object parsedFormat = parsedMap.get("format");
                            if (parsedFormat != null && parsedFormat.equals(format) == false) {
                                map.put("format", parsedFormat);
                                anyChanges = true;
                            }
                        } else {
                            var anyNestedChanges = upgradeDateFormatProperties(map, parsedMap);
                            anyChanges = anyChanges || anyNestedChanges;
                        }
                    }
                }
            }
        }

        return anyChanges;
    }

    /**
     * Upgrades the date format field of mapping properties
     * <p>
     * This method checks if the new mapping source has a different format than the original mapping, and if it
     * does, then it returns the original mapping source with updated format field. We cannot simply replace the
     * MappingMetadata mappingSource with the new parsedSource, because the parsed source contains additional
     * properties which cannot be serialized and reread.
     *
     * @param mappingMetadata the current mapping metadata
     * @param parsedSource the mapping source as it was parsed
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static CompressedXContent upgradeDateFormatsIfNeeded(MappingMetadata mappingMetadata, CompressedXContent parsedSource) {
        Map<String, Object> sourceMap = mappingMetadata.rawSourceAsMap();
        Map<String, Object> newMap = XContentHelper.convertToMap(parsedSource.compressedReference(), true).v2();

        Map<String, Object> mappings = getMappingsForType(sourceMap, mappingMetadata.type());
        Map<String, Object> parsedMappings = getMappingsForType(newMap, mappingMetadata.type());

        if (mappings == null || parsedMappings == null) {
            return mappingMetadata.source();
        }

        boolean anyPropertiesChanges = upgradeDateFormatProperties(
            (Map<String, Object>) mappings.get("properties"),
            (Map<String, Object>) parsedMappings.get("properties")
        );

        boolean anyRuntimeChanges = upgradeDateFormatProperties(
            (Map<String, Object>) mappings.get("runtime"),
            (Map<String, Object>) parsedMappings.get("runtime")
        );

        if (anyRuntimeChanges == false && anyPropertiesChanges == false) {
            return mappingMetadata.source();
        }

        try {
            return new CompressedXContent(sourceMap);
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected error remapping source map", e);
        }
    }
}
