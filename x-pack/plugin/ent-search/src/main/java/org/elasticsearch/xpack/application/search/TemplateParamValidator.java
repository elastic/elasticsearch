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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Set;

public class TemplateParamValidator implements ToXContentObject, Writeable {

    private static final SpecVersion.VersionFlag SCHEMA_VERSION = SpecVersion.VersionFlag.V7;
    private static final JsonSchemaFactory SCHEMA_FACTORY = JsonSchemaFactory.getInstance(SCHEMA_VERSION);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonSchema META_SCHEMA = SCHEMA_FACTORY.getSchema(
        TemplateParamValidator.class.getResourceAsStream("json-schema-draft-07.json")
    );

    private static final ConstructingObjectParser<TemplateParamValidator, Void> PARSER = new ConstructingObjectParser<>(
        "param_validation",
        p -> new TemplateParamValidator((String) p[0])
    );

    private final JsonSchema jsonSchema;

    public TemplateParamValidator(StreamInput in) throws IOException {
        this(in.readString());
    }

    public TemplateParamValidator(String validationSource) throws ValidationException {
        try {
            final JsonNode schemaJsonNode = OBJECT_MAPPER.readTree(validationSource);
            final Set<ValidationMessage> validationMessages = META_SCHEMA.validate(schemaJsonNode);

            if (validationMessages.isEmpty() == false) {
                ValidationException validationException = new ValidationException();
                for (ValidationMessage validationMessage : validationMessages) {
                    validationException.addValidationError(validationMessage.getMessage());
                }

                throw validationException;
            }

            this.jsonSchema = SCHEMA_FACTORY.getSchema(schemaJsonNode);
        } catch (JsonProcessingException e) {
            throw new ValidationException().addValidationError(e.getMessage());
        }
    }

    public TemplateParamValidator(XContentBuilder xContentBuilder) {
        this(Strings.toString(xContentBuilder));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        try (InputStream stream = new BytesArray(getSchemaAsString()).streamInput()) {
            builder.rawValue(stream, builder.contentType());
        }

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getSchemaAsString());
    }

    private String getSchemaAsString() {
        return jsonSchema.getSchemaNode().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TemplateParamValidator that = (TemplateParamValidator) o;
        return Objects.equals(getSchemaAsString(), that.getSchemaAsString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSchemaAsString());
    }
}
