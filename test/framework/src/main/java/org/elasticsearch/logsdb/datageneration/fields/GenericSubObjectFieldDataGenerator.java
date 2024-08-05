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
import org.elasticsearch.logsdb.datageneration.fields.leaf.ByteFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.DoubleFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.FloatFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.HalfFloatFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.IntegerFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.KeywordFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.LongFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.ScaledFloatFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.ShortFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.UnsignedLongFieldDataGenerator;
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
        var childFieldsCount = context.childFieldGenerator().generateChildFieldCount();
        var result = new ArrayList<ChildField>(childFieldsCount);

        for (int i = 0; i < childFieldsCount; i++) {
            var fieldName = generateFieldName(existingFieldNames);

            if (context.shouldAddObjectField()) {
                result.add(new ChildField(fieldName, new ObjectFieldDataGenerator(context.subObject())));
            } else if (context.shouldAddNestedField()) {
                result.add(new ChildField(fieldName, new NestedFieldDataGenerator(context.nestedObject())));
            } else {
                var fieldType = context.fieldTypeGenerator().generator().get();
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

    private ChildField leafField(FieldType type, String fieldName) {
        var generator = switch (type) {
            case KEYWORD -> new KeywordFieldDataGenerator(context.specification().dataSource());
            case LONG -> new LongFieldDataGenerator(context.specification().dataSource());
            case UNSIGNED_LONG -> new UnsignedLongFieldDataGenerator(context.specification().dataSource());
            case INTEGER -> new IntegerFieldDataGenerator(context.specification().dataSource());
            case SHORT -> new ShortFieldDataGenerator(context.specification().dataSource());
            case BYTE -> new ByteFieldDataGenerator(context.specification().dataSource());
            case DOUBLE -> new DoubleFieldDataGenerator(context.specification().dataSource());
            case FLOAT -> new FloatFieldDataGenerator(context.specification().dataSource());
            case HALF_FLOAT -> new HalfFloatFieldDataGenerator(context.specification().dataSource());
            case SCALED_FLOAT -> new ScaledFloatFieldDataGenerator(context.specification().dataSource());
        };

        return new ChildField(fieldName, generator);
    }

    private String generateFieldName(Set<String> existingFields) {
        var fieldName = context.childFieldGenerator().generateFieldName();
        while (existingFields.contains(fieldName)) {
            fieldName = context.childFieldGenerator().generateFieldName();
        }
        existingFields.add(fieldName);

        return fieldName;
    }

    record ChildField(String fieldName, FieldDataGenerator generator) {}
}
