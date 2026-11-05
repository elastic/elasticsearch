/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.xcontent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Validates JSON test output against an in-memory JSON Schema without exposing
 * the third-party validation implementation to consumers.
 */
public final class JsonSchemaTestValidator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final JsonSchema schema;

    private JsonSchemaTestValidator(JsonSchema schema) {
        this.schema = schema;
    }

    /**
     * Creates a validator for an in-memory JSON Schema Draft 7 document.
     */
    public static JsonSchemaTestValidator draft7(Map<String, Object> schema) {
        return new JsonSchemaTestValidator(
            JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(OBJECT_MAPPER.valueToTree(schema))
        );
    }

    /**
     * Returns validation errors for the supplied JSON document.
     */
    public List<String> validate(BytesReference json) throws IOException {
        return schema.validate(OBJECT_MAPPER.readTree(json.streamInput())).stream().map(Object::toString).sorted().toList();
    }
}
