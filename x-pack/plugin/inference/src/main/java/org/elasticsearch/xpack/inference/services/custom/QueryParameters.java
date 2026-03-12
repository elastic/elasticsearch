/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalListOfStringTuples;

public record QueryParameters(List<Parameter> parameters) implements ToXContentFragment, Writeable {

    public static final QueryParameters EMPTY = new QueryParameters(List.of());
    public static final String QUERY_PARAMETERS = "query_parameters";

    public static QueryParameters fromMap(Map<String, Object> map, ValidationException validationException) {
        List<Tuple<String, String>> queryParams = extractOptionalListOfStringTuples(
            map,
            QUERY_PARAMETERS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return QueryParameters.fromTuples(queryParams);
    }

    private static QueryParameters fromTuples(List<Tuple<String, String>> queryParams) {
        if (queryParams == null) {
            return QueryParameters.EMPTY;
        }

        return new QueryParameters(queryParams.stream().map((tuple) -> new Parameter(tuple.v1(), tuple.v2())).toList());
    }

    public record Parameter(String key, String value) implements ToXContentFragment, Writeable {
        public Parameter {
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);
        }

        public Parameter(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeString(value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            builder.value(key);
            builder.value(value);
            builder.endArray();
            return builder;
        }
    }

    public QueryParameters {
        Objects.requireNonNull(parameters);
    }

    public QueryParameters(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(Parameter::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(parameters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (parameters.isEmpty() == false) {
            builder.startArray(QUERY_PARAMETERS);
            for (var parameter : parameters) {
                parameter.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder;
    }
}
