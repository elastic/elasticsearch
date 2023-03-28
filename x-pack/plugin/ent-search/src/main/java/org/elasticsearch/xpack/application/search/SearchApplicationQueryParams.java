/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SearchApplicationQueryParams implements ToXContentObject, Writeable {

    public static final ParseField TEMPLATE_PARAMS_FIELD = new ParseField("params");
    private static final ConstructingObjectParser<SearchApplicationQueryParams, Void> PARSER = new ConstructingObjectParser<>(
        "query_params",
        p -> {
            @SuppressWarnings("unchecked")
            final Map<String, Object> templateParams = (Map<String, Object>) p[0];
            return new SearchApplicationQueryParams(templateParams);
        }
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), TEMPLATE_PARAMS_FIELD);
    }

    private final Map<String, Object> templateParams;

    public SearchApplicationQueryParams() {
        this(Map.of());
    }

    public SearchApplicationQueryParams(Map<String, Object> templateParams) {
        Objects.requireNonNull(templateParams, "Query parameters must not be null");
        this.templateParams = templateParams;
    }

    public SearchApplicationQueryParams(StreamInput in) throws IOException {
        this.templateParams = in.readMap();
    }

    public static SearchApplicationQueryParams parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public Map<String, Object> templateParams() {
        return templateParams;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEMPLATE_PARAMS_FIELD.getPreferredName(), templateParams);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(templateParams);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(templateParams);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchApplicationQueryParams searchApplicationQueryParam = (SearchApplicationQueryParams) o;
        if (templateParams == null) return searchApplicationQueryParam.templateParams == null;
        return templateParams.equals(searchApplicationQueryParam.templateParams);
    }
}
