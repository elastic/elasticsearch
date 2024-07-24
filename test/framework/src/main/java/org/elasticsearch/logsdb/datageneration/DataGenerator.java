/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.logsdb.datageneration.fields.ObjectFieldDataGenerator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Entry point of data generation logic.
 * Every instance of generator generates a random mapping and a document generation routine
 * that produces randomly generated documents valid for this mapping.
 */
public class DataGenerator {
    private final FieldDataGenerator topLevelGenerator;

    public DataGenerator(DataGeneratorSpecification specification) {
        this.topLevelGenerator = new ObjectFieldDataGenerator(specification);
    }

    public void writeMapping(XContentBuilder mapping) throws IOException {
        mapping.startObject().field("_doc");
        topLevelGenerator.mappingWriter().accept(mapping);
        mapping.endObject();
    }

    public void generateDocument(XContentBuilder document) throws IOException {
        topLevelGenerator.fieldValueGenerator().accept(document);
    }
}
