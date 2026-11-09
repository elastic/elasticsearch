/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.profileschema;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes the complete ES|QL profile response schema as a standalone draft-7 JSON document.
 */
public final class GenerateProfileSchema {

    private GenerateProfileSchema() {}

    /**
     * Generate the schema at the sole command-line path.
     * @param args the output path
     * @throws IOException if the schema cannot be written
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("expected the output schema path");
        }
        Path output = Path.of(args[0]);
        Files.createDirectories(output.getParent());
        try (OutputStream stream = Files.newOutputStream(output); XContentBuilder builder = XContentFactory.jsonBuilder(stream)) {
            builder.prettyPrint();
            builder.map(EsqlProfileSchemaDocument.create());
        }
    }
}
