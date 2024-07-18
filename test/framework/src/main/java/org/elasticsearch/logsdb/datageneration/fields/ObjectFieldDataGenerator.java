/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ObjectFieldDataGenerator implements FieldDataGenerator {
    private final DataGeneratorSpecification specification;
    private final int depth;

    private final List<ChildField> childFields;

    public ObjectFieldDataGenerator(DataGeneratorSpecification specification, int depth) {
        this.specification = specification;
        this.depth = depth;
        this.childFields = new ArrayList<>();
        generateChildFields();
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
        return b -> {
            b.startObject().startObject("properties");

            for (var childField : childFields) {
                b.field(childField.fieldName);
                childField.generator.mappingWriter().accept(b);
            }

            b.endObject().endObject();
        };
    }

    @Override
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
        var childFieldsCount = specification.arbitrary().childFieldCount(0, specification.maxFieldCountPerLevel());

        for (int i = 0; i < childFieldsCount; i++) {
            var fieldName = generateFieldName(existingFields);

            if (specification.arbitrary().generateSubObject() && depth < specification.maxObjectDepth()) {
                childFields.add(new ChildField(fieldName, new ObjectFieldDataGenerator(specification, depth + 1)));
            } else {
                var fieldType = specification.arbitrary().fieldType();
                addLeafField(fieldType, fieldName);
            }
        }
    }

    private void addLeafField(FieldType type, String fieldName) {
        var generator = switch (type) {
            case LONG -> new LongFieldDataGenerator(specification.arbitrary());
            case KEYWORD -> new KeywordFieldDataGenerator(specification.arbitrary());
        };

        childFields.add(new ChildField(fieldName, generator));
    }

    private String generateFieldName(Set<String> existingFields) {
        var fieldName = specification.arbitrary().fieldName(1, 10);
        while (existingFields.contains(fieldName)) {
            fieldName = specification.arbitrary().fieldName(1, 10);
        }
        existingFields.add(fieldName);

        return fieldName;
    }

    private record ChildField(String fieldName, FieldDataGenerator generator) {}
}
