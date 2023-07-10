/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Contains the template parameter validation, as a JSON Schema. It validates that the JSON schema
 * is valid, and will apply it to the query parameters
 */
public class TemplateParamValidator implements ToXContentObject, Writeable {

    private static final SpecVersion.VersionFlag SCHEMA_VERSION = SpecVersion.VersionFlag.V7;
    private static final JsonSchemaFactory SCHEMA_FACTORY = JsonSchemaFactory.getInstance(SCHEMA_VERSION);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonSchema META_SCHEMA = SCHEMA_FACTORY.getSchema(
        TemplateParamValidator.class.getResourceAsStream("json-schema-draft-07.json")
    );
    private final JsonSchema jsonSchema;

    public TemplateParamValidator(StreamInput in) throws IOException {
        this(in.readString());
    }

    public TemplateParamValidator(String dictionaryContent) throws ValidationException {
        try {
            // Create a new Schema with "properties" node based on the dictionary content
            final JsonNode schemaJsonNode = OBJECT_MAPPER.readTree(dictionaryContent);
            validateWithSchema(META_SCHEMA, schemaJsonNode);

            this.jsonSchema = SCHEMA_FACTORY.getSchema(schemaJsonNode);
        } catch (JsonProcessingException e) {
            throw new ValidationException().addValidationError(e.getMessage());
        }
    }

    public TemplateParamValidator(XContentBuilder xContentBuilder) {
        this(Strings.toString(xContentBuilder));
    }

    private static void validateWithSchema(JsonSchema jsonSchema, JsonNode jsonNode) {
        final Set<ValidationMessage> validationMessages = jsonSchema.validate(jsonNode);
        if (validationMessages.isEmpty() == false) {
            ValidationException validationException = new ValidationException();
            for (ValidationMessage message : validationMessages) {
                validationException.addValidationError(message.getMessage());
            }

            throw validationException;
        }
    }

    public void validate(Map<String, Object> templateParams) throws ValidationException {
        validateWithSchema(this.jsonSchema, OBJECT_MAPPER.valueToTree(templateParams));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        try (InputStream stream = new BytesArray(getSchemaPropertiesAsString()).streamInput()) {
            builder.rawValue(stream, XContentType.JSON);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getSchemaPropertiesAsString());
    }

    private String getSchemaPropertiesAsString() {
        return jsonSchema.getSchemaNode().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TemplateParamValidator that = (TemplateParamValidator) o;
        return Objects.equals(getSchemaPropertiesAsString(), that.getSchemaPropertiesAsString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSchemaPropertiesAsString());
    }
}
