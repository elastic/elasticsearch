/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logsdb.datageneration.fields.TopLevelObjectFieldDataGenerator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Entry point of data generation logic.
 * Every instance of generator generates a random mapping and a document generation routine
 * that produces randomly generated documents valid for this mapping.
 */
public class DataGenerator {
    private final TopLevelObjectFieldDataGenerator topLevelGenerator;

    public DataGenerator(DataGeneratorSpecification specification) {
        this.topLevelGenerator = new TopLevelObjectFieldDataGenerator(specification);
    }

    /**
     * Writes a fully built mapping document (enclosed in a top-level object) to a provided builder.
     * @param mapping destination
     * @throws IOException
     */
    public void writeMapping(XContentBuilder mapping) throws IOException {
        mapping.startObject().field("_doc");
        topLevelGenerator.mappingWriter(b -> {}).accept(mapping);
        mapping.endObject();
    }

    /**
     * Writes a fully built mapping document (enclosed in a top-level object) to a provided builder.
     * Allows customizing parameters of top level object mapper.
     * @param mapping destination
     * @param customMappingParameters writer of custom mapping parameters of top level object mapping
     * @throws IOException
     */
    public void writeMapping(XContentBuilder mapping, CheckedConsumer<XContentBuilder, IOException> customMappingParameters)
        throws IOException {
        mapping.startObject().field("_doc");
        topLevelGenerator.mappingWriter(customMappingParameters).accept(mapping);
        mapping.endObject();
    }

    /**
     * Generates a document and writes it to a provided builder. New document is generated every time.
     * @param document
     * @throws IOException
     */
    public void generateDocument(XContentBuilder document) throws IOException {
        topLevelGenerator.fieldValueGenerator(b -> {}).accept(document);
    }

    /**
     * Generates a document and writes it to a provided builder. New document is generated every time.
     * Supports appending custom content to generated document (e.g. a custom generated field).
     * @param document
     * @param customDocumentModifications
     * @throws IOException
     */
    public void generateDocument(XContentBuilder document, CheckedConsumer<XContentBuilder, IOException> customDocumentModifications)
        throws IOException {
        topLevelGenerator.fieldValueGenerator(customDocumentModifications).accept(document);
    }
}
