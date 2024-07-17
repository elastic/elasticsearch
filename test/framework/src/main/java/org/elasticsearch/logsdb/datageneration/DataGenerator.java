/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logsdb.datageneration.fields.KeywordFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.LongFieldDataGenerator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Entry point of data generation logic.
 * Every instance of generator generates a random mapping and a document generation routine
 * that produces randomly generated documents valid for that mapping.
 */
public class DataGenerator {
    private final List<CheckedConsumer<XContentBuilder, IOException>> mappingModifications;
    private final List<CheckedConsumer<XContentBuilder, IOException>> documentModifications;

    private final DataGeneratorSpecification specification;

    private static final int MAX_OBJECT_DEPTH = 4;

    public DataGenerator(DataGeneratorSpecification specification) {
        this.specification = specification;
        this.mappingModifications = new ArrayList<>();
        this.documentModifications = new ArrayList<>();
        generate();
    }

    public void writeMapping(XContentBuilder mapping) throws IOException {
        applyAll(mappingModifications, mapping);
    }

    public void generateDocument(XContentBuilder document) throws IOException {
        applyAll(documentModifications, document);
    }

    private void generate() {
        mappingModifications.add(b -> b.startObject().startObject("_doc").startObject("properties"));
        documentModifications.add(b -> b.startObject());

        // Start with top-level object.
        generateObject(0);

        mappingModifications.add(b -> b.endObject().endObject().endObject());
        documentModifications.add(b -> b.endObject());
    }

    private void generateObject(int depth) {
        // Deeply nested objects have fewer and fewer fields
        var depthLevelChange = specification.maxFieldCountPerLevel() / MAX_OBJECT_DEPTH;
        int maxFieldCount = Math.max(1, specification.maxFieldCountPerLevel() - depthLevelChange * depth);
        int fieldsCount = randomIntBetween(0, maxFieldCount);

        boolean hasSubObject = false;
        var existingFields = new HashSet<>();
        for (int i = 0; i < fieldsCount; i++) {
            var fieldType = randomFrom(FieldType.values());
            // Don't generate duplicate fields
            var fieldName = randomAlphaOfLengthBetween(1, 50);
            while (existingFields.contains(fieldName)) {
                fieldName = randomAlphaOfLengthBetween(1, 50);
            }
            existingFields.add(fieldName);
            var actualFieldName = fieldName;

            // Roll separately for subobjects with a 10% change but at least once
            if ((depth == 0 && hasSubObject == false) || (randomDouble() < 0.1 && depth < MAX_OBJECT_DEPTH)) {
                hasSubObject = true;

                mappingModifications.add(b -> b.startObject(actualFieldName).startObject("properties"));
                documentModifications.add(b -> b.startObject(actualFieldName));

                generateObject(depth + 1);

                mappingModifications.add(b -> b.endObject().endObject());
                documentModifications.add(b -> b.endObject());
            } else {
                generateField(fieldType, fieldName);
            }
        }
    }

    private void generateField(FieldType type, String fieldName) {
        var generator = switch (type) {
            case LONG -> new LongFieldDataGenerator(fieldName);
            case KEYWORD -> new KeywordFieldDataGenerator(fieldName);
        };

        mappingModifications.add(generator.mappingWriter());
        documentModifications.add(generator.fieldValueGenerator());
    }

    private static <T, E extends Exception> void applyAll(Collection<CheckedConsumer<T, E>> consumers, T target) throws E {
        for (var consumer : consumers) {
            consumer.accept(target);
        }
    }
}
