/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ReverseNestedAggregationBuilder extends AbstractAggregationBuilder<ReverseNestedAggregationBuilder> {
    public static final String NAME = "reverse_nested";

    private String path;

    public ReverseNestedAggregationBuilder(String name) {
        super(name);
    }

    public ReverseNestedAggregationBuilder(ReverseNestedAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> map) {
        super(clone, factoriesBuilder, map);
        this.path = clone.path;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ReverseNestedAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public ReverseNestedAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        path = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(path);
    }

    /**
     * Set the path to use for this nested aggregation. The path must match
     * the path to a nested object in the mappings. If it is not specified
     * then this aggregation will go back to the root document.
     */
    public ReverseNestedAggregationBuilder path(String path) {
        if (path == null) {
            throw new IllegalArgumentException("[path] must not be null: [" + name + "]");
        }
        this.path = path;
        return this;
    }

    /**
     * Get the path to use for this nested aggregation.
     */
    public String path() {
        return path;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        if (findNestedAggregatorFactory(parent) == null) {
            throw new IllegalArgumentException("Reverse nested aggregation [" + name + "] can only be used inside a [nested] aggregation");
        }

        ObjectMapper parentObjectMapper = null;
        if (path != null) {
            parentObjectMapper = context.getObjectMapper(path);
            if (parentObjectMapper == null) {
                return new ReverseNestedAggregatorFactory(name, true, null, context, parent, subFactoriesBuilder, metadata);
            }
            if (parentObjectMapper.isNested() == false) {
                throw new AggregationExecutionException("[reverse_nested] nested path [" + path + "] is not nested");
            }
        }

        NestedScope nestedScope = context.nestedScope();
        NestedObjectMapper nestedMapper = (NestedObjectMapper) parentObjectMapper;
        try {
            nestedScope.nextLevel(nestedMapper);
            return new ReverseNestedAggregatorFactory(name, false, nestedMapper, context, parent, subFactoriesBuilder, metadata);
        } finally {
            nestedScope.previousLevel();
        }
    }

    private static NestedAggregatorFactory findNestedAggregatorFactory(AggregatorFactory parent) {
        if (parent == null) {
            return null;
        } else if (parent instanceof NestedAggregatorFactory) {
            return (NestedAggregatorFactory) parent;
        } else {
            return findNestedAggregatorFactory(parent.getParent());
        }
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (path != null) {
            builder.field(ReverseNestedAggregator.PATH_FIELD.getPreferredName(), path);
        }
        builder.endObject();
        return builder;
    }

    public static ReverseNestedAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        String path = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("path".equals(currentFieldName)) {
                    path = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "]."
                    );
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        ReverseNestedAggregationBuilder factory = new ReverseNestedAggregationBuilder(aggregationName);
        if (path != null) {
            factory.path(path);
        }
        return factory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ReverseNestedAggregationBuilder other = (ReverseNestedAggregationBuilder) obj;
        return Objects.equals(path, other.path);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
