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
import org.elasticsearch.logsdb.datageneration.fields.leaf.KeywordFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.LongFieldDataGenerator;
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

    List<ChildField> generateChildFields() {
        var existingFieldNames = new HashSet<String>();
        // no child fields is legal
        var childFieldsCount = context.specification().arbitrary().childFieldCount(0, context.specification().maxFieldCountPerLevel());
        var result = new ArrayList<ChildField>(childFieldsCount);

        for (int i = 0; i < childFieldsCount; i++) {
            var fieldName = generateFieldName(existingFieldNames);

            if (context.shouldAddObjectField()) {
                result.add(new ChildField(fieldName, new ObjectFieldDataGenerator(context.subObject())));
            } else if (context.shouldAddNestedField()) {
                result.add(new ChildField(fieldName, new NestedFieldDataGenerator(context.nestedObject())));
            } else {
                var fieldType = context.specification().arbitrary().fieldType();
                result.add(leafField(fieldType, fieldName));
            }
        }

        return result;
    }

    List<ChildField> generateChildFields(List<PredefinedField> predefinedFields) {
        return predefinedFields.stream().map(pf -> leafField(pf.fieldType(), pf.fieldName())).toList();
    }

    static void writeChildFieldsMapping(XContentBuilder mapping, List<ChildField> childFields) throws IOException {
        for (var childField : childFields) {
            mapping.field(childField.fieldName);
            childField.generator.mappingWriter().accept(mapping);
        }
    }

    static void writeObjectsData(XContentBuilder document, Context context, CheckedConsumer<XContentBuilder, IOException> objectWriter)
        throws IOException {
        if (context.shouldGenerateObjectArray()) {
            int size = context.specification().arbitrary().objectArraySize();

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

    private ChildField leafField(FieldType type, String fieldName) {
        var generator = switch (type) {
            case LONG -> new LongFieldDataGenerator(context.specification().arbitrary());
            case KEYWORD -> new KeywordFieldDataGenerator(context.specification().arbitrary());
        };

        return new ChildField(fieldName, generator);
    }

    private String generateFieldName(Set<String> existingFields) {
        var fieldName = context.specification().arbitrary().fieldName(1, 10);
        while (existingFields.contains(fieldName)) {
            fieldName = context.specification().arbitrary().fieldName(1, 10);
        }
        existingFields.add(fieldName);

        return fieldName;
    }

    record ChildField(String fieldName, FieldDataGenerator generator) {}
}
