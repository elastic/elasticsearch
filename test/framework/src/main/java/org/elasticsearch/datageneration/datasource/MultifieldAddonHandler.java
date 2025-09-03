/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultifieldAddonHandler implements DataSourceHandler {

    private static final String PLACEHOLDER = DefaultObjectGenerationHandler.RESERVED_FIELD_NAME_PREFIX + "multifield";
    private static final float DEFAULT_CHANCE_OF_CHILD_FIELD = 0.5f;
    private final Map<FieldType, List<FieldType>> subfieldTypes;
    private final float chanceOfChildField;

    private static final List<FieldType> STRING_TYPES = List.of(
        FieldType.TEXT,
        FieldType.KEYWORD,
        FieldType.MATCH_ONLY_TEXT,
        FieldType.WILDCARD
    );
    public static MultifieldAddonHandler STRING_TYPE_HANDLER = new MultifieldAddonHandler(
        STRING_TYPES.stream().collect(Collectors.toMap(t -> t, t -> STRING_TYPES.stream().filter(s -> s != t).toList()))
    );

    public MultifieldAddonHandler(Map<FieldType, List<FieldType>> subfieldTypes, float chanceOfChildField) {
        this.subfieldTypes = subfieldTypes;
        this.chanceOfChildField = chanceOfChildField;
    }

    public MultifieldAddonHandler(Map<FieldType, List<FieldType>> subfieldTypes) {
        this(subfieldTypes, DEFAULT_CHANCE_OF_CHILD_FIELD);
    }

    @Override
    public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {

        // Need to delegate creation of the same type of field to other handlers. So skip request
        // if it's for the placeholder name used when creating the child and parent fields.
        if (request.fieldName().equals(PLACEHOLDER)) {
            return null;
        }

        FieldType parentType = FieldType.tryParse(request.fieldType());
        List<FieldType> childTypes = subfieldTypes.get(parentType);
        if (childTypes == null) {
            return null;
        }

        return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
            assert parentType != null;
            var parent = getMappingForType(parentType, request);
            if (ESTestCase.randomFloat() > chanceOfChildField) {
                return parent;
            }

            var childType = ESTestCase.randomFrom(childTypes);
            var child = getChildMappingForType(childType, request);

            child.put("type", childType.toString());
            String childName = "subfield_" + childType;
            parent.put("fields", Map.of(childName, child));
            return parent;
        });
    }

    private static Map<String, Object> getChildMappingForType(FieldType type, DataSourceRequest.LeafMappingParametersGenerator request) {
        Map<String, Object> mapping = getMappingForType(type, request);
        mapping.remove("copy_to");
        return mapping;
    }

    private static Map<String, Object> getMappingForType(FieldType type, DataSourceRequest.LeafMappingParametersGenerator request) {
        return request.dataSource()
            .get(
                new DataSourceRequest.LeafMappingParametersGenerator(
                    request.dataSource(),
                    PLACEHOLDER,
                    type.toString(),
                    request.eligibleCopyToFields(),
                    request.dynamicMapping()
                )
            )
            .mappingGenerator()
            .get();
    }
}
