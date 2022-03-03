/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import java.util.Map;
import java.util.function.BiConsumer;

public final class MappingVisitor {

    private MappingVisitor() {}

    public static void visitMapping(Map<String, ?> mapping, BiConsumer<String, Map<String, ?>> fieldMappingConsumer) {
        visitMapping(mapping, "", fieldMappingConsumer);
    }

    private static void visitMapping(
        final Map<String, ?> mapping,
        final String path,
        final BiConsumer<String, Map<String, ?>> fieldMappingConsumer
    ) {
        Object properties = mapping.get("properties");
        if (properties instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> propertiesAsMap = (Map<String, ?>) properties;
            for (Map.Entry<String, ?> entry : propertiesAsMap.entrySet()) {
                final Object v = entry.getValue();
                if (v instanceof Map) {

                    @SuppressWarnings("unchecked")
                    Map<String, ?> fieldMapping = (Map<String, ?>) v;
                    final String prefix = path + entry.getKey();
                    fieldMappingConsumer.accept(prefix, fieldMapping);
                    visitMapping(fieldMapping, prefix + ".", fieldMappingConsumer);

                    // Multi fields
                    Object fieldsO = fieldMapping.get("fields");
                    if (fieldsO instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> fields = (Map<String, ?>) fieldsO;
                        for (Map.Entry<String, ?> subfieldEntry : fields.entrySet()) {
                            Object v2 = subfieldEntry.getValue();
                            if (v2 instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, ?> fieldMapping2 = (Map<String, ?>) v2;
                                fieldMappingConsumer.accept(prefix + "." + subfieldEntry.getKey(), fieldMapping2);
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
}
