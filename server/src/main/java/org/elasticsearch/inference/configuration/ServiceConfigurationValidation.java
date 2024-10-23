/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents a configuration validation entity, encapsulating a validation constraint and its corresponding type.
 * This class is used to define and handle specific validation rules or requirements within a configuration context.
 */
public class ServiceConfigurationValidation implements Writeable, ToXContentObject {

    private final Object constraint;
    private final ServiceConfigurationValidationType type;

    /**
     * Constructs a new ServiceConfigurationValidation instance with specified constraint and type.
     * This constructor initializes the object with a given validation constraint and its associated validation type.
     *
     * @param constraint The validation constraint (string, number or list), represented as generic Object type.
     * @param type       The type of configuration validation, specified as an instance of {@link ServiceConfigurationValidationType}.
     */
    private ServiceConfigurationValidation(Object constraint, ServiceConfigurationValidationType type) {
        this.constraint = constraint;
        this.type = type;
    }

    public ServiceConfigurationValidation(StreamInput in) throws IOException {
        this.constraint = in.readGenericValue();
        this.type = in.readEnum(ServiceConfigurationValidationType.class);
    }

    private static final ParseField CONSTRAINT_FIELD = new ParseField("constraint");
    private static final ParseField TYPE_FIELD = new ParseField("type");

    private static final ConstructingObjectParser<ServiceConfigurationValidation, Void> PARSER = new ConstructingObjectParser<>(
        "service_configuration_validation",
        true,
        args -> new ServiceConfigurationValidation.Builder().setConstraint(args[0])
            .setType((ServiceConfigurationValidationType) args[1])
            .build()
    );

    static {
        PARSER.declareField(
            constructorArg(),
            (p, c) -> parseConstraintValue(p),
            CONSTRAINT_FIELD,
            ObjectParser.ValueType.VALUE_OBJECT_ARRAY
        );
        PARSER.declareField(
            constructorArg(),
            (p, c) -> ServiceConfigurationValidationType.validationType(p.text()),
            TYPE_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    /**
     * Parses the value of a constraint from the XContentParser stream.
     * This method is designed to handle various types of constraint values as per the connector's protocol original specification.
     * The constraints can be of type string, number, or list of values.
     */
    private static Object parseConstraintValue(XContentParser p) throws IOException {
        if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
            return p.text();
        } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            return p.numberValue();
        } else if (p.currentToken() == XContentParser.Token.START_ARRAY) {
            return p.list();
        }
        throw new XContentParseException("Unsupported token [" + p.currentToken() + "]");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(CONSTRAINT_FIELD.getPreferredName(), constraint);
            builder.field(TYPE_FIELD.getPreferredName(), type.toString());
        }
        builder.endObject();
        return builder;
    }

    public Map<String, Object> toMap() {
        return Map.of(CONSTRAINT_FIELD.getPreferredName(), constraint, TYPE_FIELD.getPreferredName(), type.toString());
    }

    public static ServiceConfigurationValidation fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(constraint);
        out.writeEnum(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceConfigurationValidation that = (ServiceConfigurationValidation) o;
        return Objects.equals(constraint, that.constraint) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(constraint, type);
    }

    public static class Builder {

        private Object constraint;
        private ServiceConfigurationValidationType type;

        public Builder setConstraint(Object constraint) {
            this.constraint = constraint;
            return this;
        }

        public Builder setType(ServiceConfigurationValidationType type) {
            this.type = type;
            return this;
        }

        public ServiceConfigurationValidation build() {
            return new ServiceConfigurationValidation(constraint, type);
        }
    }
}
