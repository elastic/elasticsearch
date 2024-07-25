/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
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

    private final List<ChildField> childFields;

    public GenericSubObjectFieldDataGenerator(Context context) {
        this.context = context;

        childFields = new ArrayList<>();
        generateChildFields();
    }

    public CheckedConsumer<XContentBuilder, IOException> mappingWriter(
        CheckedConsumer<XContentBuilder, IOException> customMappingParameters
    ) {
        return b -> {
            b.startObject();
            customMappingParameters.accept(b);

            b.startObject("properties");
            for (var childField : childFields) {
                b.field(childField.fieldName);
                childField.generator.mappingWriter().accept(b);
            }
            b.endObject();

            b.endObject();
        };
    }

    public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
        return b -> {
            b.startObject();

            for (var childField : childFields) {
                b.field(childField.fieldName);
                childField.generator.fieldValueGenerator().accept(b);
            }

            b.endObject();
        };
    }

    private void generateChildFields() {
        var existingFields = new HashSet<String>();
        // no child fields is legal
        var childFieldsCount = context.specification().arbitrary().childFieldCount(0, context.specification().maxFieldCountPerLevel());

        for (int i = 0; i < childFieldsCount; i++) {
            var fieldName = generateFieldName(existingFields);

            if (context.shouldAddObjectField()) {
                childFields.add(new ChildField(fieldName, new ObjectFieldDataGenerator(context.subObject())));
            } else if (context.shouldAddNestedField()) {
                childFields.add(new ChildField(fieldName, new NestedFieldDataGenerator(context.nestedObject())));
            } else {
                var fieldType = context.specification().arbitrary().fieldType();
                addLeafField(fieldType, fieldName);
            }
        }
    }

    private void addLeafField(FieldType type, String fieldName) {
        var generator = switch (type) {
            case LONG -> new LongFieldDataGenerator(context.specification().arbitrary());
            case KEYWORD -> new KeywordFieldDataGenerator(context.specification().arbitrary());
        };

        childFields.add(new ChildField(fieldName, generator));
    }

    private String generateFieldName(Set<String> existingFields) {
        var fieldName = context.specification().arbitrary().fieldName(1, 10);
        while (existingFields.contains(fieldName)) {
            fieldName = context.specification().arbitrary().fieldName(1, 10);
        }
        existingFields.add(fieldName);

        return fieldName;
    }

    private record ChildField(String fieldName, FieldDataGenerator generator) {}
}
