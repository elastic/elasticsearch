/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * An implementation of {@link Aggregation} that is parsed from a REST response.
 * Serves as a base class for all aggregation implementations that are parsed from REST.
 */
public abstract class ParsedAggregation implements Aggregation, ToXContentFragment {

    protected static void declareAggregationFields(AbstractObjectParser<? extends ParsedAggregation, ?> objectParser) {
        objectParser.declareObject(
            (parsedAgg, metadata) -> parsedAgg.metadata = Collections.unmodifiableMap(metadata),
            (parser, context) -> parser.map(),
            InternalAggregation.CommonFields.META
        );
    }

    private String name;
    protected Map<String, Object> metadata;

    @Override
    public final String getName() {
        return name;
    }

    protected void setName(String name) {
        this.name = name;
    }

    @Override
    public final Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // Concatenates the type and the name of the aggregation (ex: top_hits#foo)
        builder.startObject(String.join(InternalAggregation.TYPED_KEYS_DELIMITER, getType(), name));
        if (this.metadata != null) {
            builder.field(InternalAggregation.CommonFields.META.getPreferredName());
            builder.map(this.metadata);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    /**
     * Parse a token of type XContentParser.Token.VALUE_NUMBER or XContentParser.Token.STRING to a double.
     * In other cases the default value is returned instead.
     */
    protected static double parseDouble(XContentParser parser, double defaultNullValue) throws IOException {
        Token currentToken = parser.currentToken();
        if (currentToken == XContentParser.Token.VALUE_NUMBER || currentToken == XContentParser.Token.VALUE_STRING) {
            return parser.doubleValue();
        } else {
            return defaultNullValue;
        }
    }
}
