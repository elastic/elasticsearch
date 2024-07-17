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

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

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
        var childFieldsCount = randomIntBetween(0, specification.maxFieldCountPerLevel());

        for (int i = 0; i < childFieldsCount; i++) {
            var fieldType = randomFrom(FieldType.values());
            var fieldName = generateFieldName(existingFields);

            // Roll separately for subobjects with a 10% chance
            if (randomDouble() < 0.1 && depth < specification.maxObjectDepth()) {
                childFields.add(new ChildField(fieldName, new ObjectFieldDataGenerator(specification, depth + 1)));
            } else {
                addLeafField(fieldType, fieldName);
            }
        }
    }

    private void addLeafField(FieldType type, String fieldName) {
        var generator = switch (type) {
            case LONG -> new LongFieldDataGenerator();
            case KEYWORD -> new KeywordFieldDataGenerator();
        };

        childFields.add(new ChildField(fieldName, generator));
    }

    private static String generateFieldName(Set<String> existingFields) {
        var fieldName = randomAlphaOfLengthBetween(1, 50);
        while (existingFields.contains(fieldName)) {
            fieldName = randomAlphaOfLengthBetween(1, 50);
        }
        existingFields.add(fieldName);

        return fieldName;
    }

    private record ChildField(String fieldName, FieldDataGenerator generator) {}
}
