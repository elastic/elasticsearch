/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Generic generator for any type of object field (e.g. "object", "nested").
 */
public class GenericSubObjectFieldDataGenerator {
    private final Context context;

    GenericSubObjectFieldDataGenerator(Context context) {
        this.context = context;
    }

    List<ChildField> generateChildFields(DynamicMapping dynamicMapping, ObjectMapper.Subobjects subobjects) {
        var existingFieldNames = new HashSet<String>();
        // no child fields is legal
        var childFieldsCount = context.childFieldGenerator().generateChildFieldCount();
        var result = new ArrayList<ChildField>(childFieldsCount);

        for (int i = 0; i < childFieldsCount; i++) {
            var fieldName = generateFieldName(existingFieldNames);

            if (context.shouldAddDynamicObjectField(dynamicMapping)) {
                result.add(
                    new ChildField(
                        fieldName,
                        new ObjectFieldDataGenerator(context.subObject(fieldName, DynamicMapping.FORCED, subobjects)),
                        true
                    )
                );
            } else if (context.shouldAddObjectField()) {
                result.add(
                    new ChildField(fieldName, new ObjectFieldDataGenerator(context.subObject(fieldName, dynamicMapping, subobjects)), false)
                );
            } else if (context.shouldAddNestedField(subobjects)) {
                result.add(
                    new ChildField(
                        fieldName,
                        new NestedFieldDataGenerator(context.nestedObject(fieldName, dynamicMapping, subobjects)),
                        false
                    )
                );
            } else {
                var fieldTypeInfo = context.fieldTypeGenerator(dynamicMapping).generator().get();

                // For simplicity we only copy to keyword fields, synthetic source logic to handle copy_to is generic.
                if (fieldTypeInfo.fieldType() == FieldType.KEYWORD) {
                    context.markFieldAsEligibleForCopyTo(fieldName);
                }

                var mappingParametersGenerator = context.specification()
                    .dataSource()
                    .get(
                        new DataSourceRequest.LeafMappingParametersGenerator(
                            fieldName,
                            fieldTypeInfo.fieldType(),
                            context.getEligibleCopyToDestinations(),
                            dynamicMapping
                        )
                    );
                var generator = fieldTypeInfo.fieldType()
                    .generator(fieldName, context.specification().dataSource(), mappingParametersGenerator);
                result.add(new ChildField(fieldName, generator, fieldTypeInfo.dynamic()));
            }
        }

        return result;
    }

    List<ChildField> generateChildFields(List<PredefinedField> predefinedFields) {
        return predefinedFields.stream()
            .map(pf -> new ChildField(pf.name(), pf.generator(context.specification().dataSource()), false))
            .toList();
    }

    static void writeChildFieldsMapping(XContentBuilder mapping, List<ChildField> childFields) throws IOException {
        for (var childField : childFields) {
            if (childField.dynamic() == false) {
                mapping.field(childField.fieldName);
                childField.generator.mappingWriter().accept(mapping);
            }
        }
    }

    static void writeObjectsData(XContentBuilder document, Context context, CheckedConsumer<XContentBuilder, IOException> objectWriter)
        throws IOException {
        var optionalLength = context.generateObjectArray();
        if (optionalLength.isPresent()) {
            int size = optionalLength.get();

            document.startArray();
            for (int i = 0; i < size; i++) {
                objectWriter.accept(document);
            }
            document.endArray();
        } else {
            objectWriter.accept(document);
        }
    }

    static void writeSingleObject(XContentBuilder document, Iterable<ChildField> childFields) throws IOException {
        document.startObject();
        writeChildFieldsData(document, childFields);
        document.endObject();
    }

    static void writeChildFieldsData(XContentBuilder document, Iterable<ChildField> childFields) throws IOException {
        for (var childField : childFields) {
            document.field(childField.fieldName);
            childField.generator.fieldValueGenerator().accept(document);
        }
    }

    private String generateFieldName(Set<String> existingFields) {
        var fieldName = context.childFieldGenerator().generateFieldName();
        while (existingFields.contains(fieldName)) {
            fieldName = context.childFieldGenerator().generateFieldName();
        }
        existingFields.add(fieldName);

        return fieldName;
    }

    record ChildField(String fieldName, FieldDataGenerator generator, boolean dynamic) {}
}
