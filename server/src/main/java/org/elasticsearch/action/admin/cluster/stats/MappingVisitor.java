/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.TriConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public final class MappingVisitor {
    public static final String PROPERTIES = "properties";
    public static final String FIELD_TYPE = "type";
    public static final String MULTI_FIELDS = "fields";

    private MappingVisitor() {}

    public static void visitMapping(Map<String, ?> mapping, BiConsumer<String, Map<String, ?>> fieldMappingConsumer) {
        visitMapping(mapping, "", fieldMappingConsumer, fieldMappingConsumer);
    }

    public static void visitMapping(
        Map<String, ?> mapping,
        BiConsumer<String, Map<String, ?>> fieldMappingConsumer,
        final BiConsumer<String, Map<String, ?>> multiFieldsMappingConsumer
    ) {
        visitMapping(mapping, "", fieldMappingConsumer, multiFieldsMappingConsumer);
    }

    private static void visitMapping(
        final Map<String, ?> mapping,
        final String path,
        final BiConsumer<String, Map<String, ?>> propertiesMappingConsumer,
        final BiConsumer<String, Map<String, ?>> multiFieldsMappingConsumer
    ) {
        Object properties = mapping.get(PROPERTIES);
        if (properties instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> propertiesAsMap = (Map<String, ?>) properties;
            for (Map.Entry<String, ?> entry : propertiesAsMap.entrySet()) {
                final Object v = entry.getValue();
                if (v instanceof Map) {

                    @SuppressWarnings("unchecked")
                    Map<String, ?> fieldMapping = (Map<String, ?>) v;
                    final String prefix = path + entry.getKey();
                    propertiesMappingConsumer.accept(prefix, fieldMapping);
                    visitMapping(fieldMapping, prefix + ".", propertiesMappingConsumer, multiFieldsMappingConsumer);

                    // Multi fields
                    Object fieldsO = fieldMapping.get(MULTI_FIELDS);
                    if (fieldsO instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> fields = (Map<String, ?>) fieldsO;
                        for (Map.Entry<String, ?> subfieldEntry : fields.entrySet()) {
                            Object v2 = subfieldEntry.getValue();
                            if (v2 instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, ?> fieldMapping2 = (Map<String, ?>) v2;
                                multiFieldsMappingConsumer.accept(prefix + "." + subfieldEntry.getKey(), fieldMapping2);
                            }
                        }
                    }
                }
            }
        }
    }

    public static void visitRuntimeMapping(Map<String, ?> mapping, BiConsumer<String, Map<String, ?>> runtimeFieldMappingConsumer) {
        Object runtimeObject = mapping.get("runtime");
        if (runtimeObject instanceof Map == false) {
            return;
        }
        @SuppressWarnings("unchecked")
        Map<String, ?> runtimeMappings = (Map<String, ?>) runtimeObject;
        for (Map.Entry<String, ?> entry : runtimeMappings.entrySet()) {
            final Object runtimeFieldMappingObject = entry.getValue();
            if (runtimeFieldMappingObject instanceof Map == false) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, ?> runtimeFieldMapping = (Map<String, ?>) runtimeFieldMappingObject;
            runtimeFieldMappingConsumer.accept(entry.getKey(), runtimeFieldMapping);
        }
    }

    /**
     * This visitor traverses the source mapping and copies the structure to the destination mapping after applying
     * the fieldMappingConsumer to the individual properties.
     */
    public static void visitPropertiesAndCopyMapping(
        final Map<String, ?> sourceMapping,
        final Map<String, Object> destMapping,
        final TriConsumer<String, Map<String, ?>, Map<String, Object>> fieldMappingConsumer
    ) {
        Map<String, ?> sourceProperties = getMapOrNull(sourceMapping.get(PROPERTIES));
        if (sourceProperties == null) {
            return;
        }
        Map<String, Object> destProperties = new HashMap<>(sourceProperties.size());
        destMapping.put(PROPERTIES, destProperties);

        for (Map.Entry<String, ?> entry : sourceProperties.entrySet()) {
            Map<String, ?> sourceFieldMapping = getMapOrNull(entry.getValue());
            if (sourceFieldMapping == null) {
                return;
            }
            var destFieldMapping = processAndCopy(entry.getKey(), sourceFieldMapping, destProperties, fieldMappingConsumer);
            visitPropertiesAndCopyMapping(sourceFieldMapping, destFieldMapping, fieldMappingConsumer);
        }
    }

    private static Map<String, ?> getMapOrNull(Object object) {
        if (object instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> map = (Map<String, ?>) object;
            return map;
        }
        return null;
    }

    private static Map<String, Object> processAndCopy(
        String fieldName,
        Map<String, ?> sourceFieldMapping,
        Map<String, Object> destParentMap,
        TriConsumer<String, Map<String, ?>, Map<String, Object>> fieldMappingConsumer
    ) {
        Map<String, Object> destFieldMapping = new HashMap<>(sourceFieldMapping.size());
        destParentMap.put(fieldName, destFieldMapping);
        fieldMappingConsumer.apply(fieldName, sourceFieldMapping, destFieldMapping);
        return destFieldMapping;
    }
}
