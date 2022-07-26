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
    // package private for testing
    static Map<String, Object> getMappingProperties(Map<String, Object> mappingMap, String type) {
        // Check for MappingMap without type and ignore it
        if (mappingMap.size() != 1) {
            return null;
        }
        // Fetch the mappings for the encoded type
        Map<String, Object> mappingType = (Map<String, Object>) mappingMap.get(type);
        if (mappingType == null) {
            return null;
        }

        return (Map<String, Object>) mappingType.get("properties");
    }

    /**
     * Upgrades the format field of mapping properties
     * <p>
     * This method checks is the new mapping source has a different format than the original mapping, and if it
     * does, then it returns the original mapping source with updated format field. We cannot simply replace the
     * MappingMetadata mappingSource with the new parsedSource, because the parsed source contains additional
     * properties which cannot be serialized and reread.
     *
     * @param mappingMetadata the current mapping metadata
     * @param parsedSource the mapping source as it was parsed
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static CompressedXContent upgradeFormatsIfNeeded(MappingMetadata mappingMetadata, CompressedXContent parsedSource) {
        Map<String, Object> sourceMap = mappingMetadata.rawSourceAsMap();
        Map<String, Object> newMap = XContentHelper.convertToMap(parsedSource.compressedReference(), true).v2();

        Map<String, Object> properties = getMappingProperties(sourceMap, mappingMetadata.type());
        Map<String, Object> parsedProperties = getMappingProperties(newMap, mappingMetadata.type());
        if (properties != null && parsedProperties != null) {
            for (var property : properties.entrySet()) {
                if (property.getValue()instanceof Map map) {
                    Object format = map.get("format");
                    if (format != null) {
                        var newProperty = parsedProperties.get(property.getKey());
                        if (newProperty instanceof Map parsedMap) {
                            Object parsedFormat = parsedMap.get("format");
                            if (parsedFormat != null && parsedFormat.equals(format) == false) {
                                map.put("format", parsedFormat);
                            }
                        }
                    }
                }
            }
        }

        try {
            return new CompressedXContent(sourceMap);
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected error remapping source map", e);
        }
    }
}
