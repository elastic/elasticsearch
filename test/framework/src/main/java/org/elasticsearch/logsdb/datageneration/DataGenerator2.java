/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class DataGenerator2 {
    private final MappingGenerator mappingGenerator;
    private final DocumentGenerator documentGenerator;

    private final MappingTemplate mappingTemplate;
    private final Mapping mapping;

    public DataGenerator2(DataGeneratorSpecification specification) {
        this.mappingGenerator = new MappingGenerator(specification);
        this.documentGenerator = new DocumentGenerator(specification);

        this.mappingTemplate = new MappingTemplateGenerator(specification).generate();
        this.mapping = new MappingGenerator(specification).generate(mappingTemplate, null);
    }

    /**
     * Writes a fully built mapping document (enclosed in a top-level object) to a provided builder.
     * @param mapping destination
     * @throws IOException
     */
    public void writeMapping(XContentBuilder mapping) throws IOException {
        mapping.startObject().field("_doc");
        mapping.map(this.mapping.raw());
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
     * @param additionalFields
     * @throws IOException
     */
    public void generateDocument(XContentBuilder document, Map<String, Object> additionalFields)
        throws IOException {
        var generated = documentGenerator.generate(mappingTemplate);
        generated.putAll(additionalFields);

        document.map(generated);
    }
}
