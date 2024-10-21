/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

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

    private static final ParseField CONSTRAINT_FIELD = new ParseField("constraint");
    private static final ParseField TYPE_FIELD = new ParseField("type");

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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(constraint);
        out.writeEnum(type);
    }

    public Map<String, Object> toMap() {
        return Map.of(CONSTRAINT_FIELD.getPreferredName(), constraint, TYPE_FIELD.getPreferredName(), type.toString());
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
