/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.aggregations.metric;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class ArrayValuesSourceAggregationBuilder<AB extends ArrayValuesSourceAggregationBuilder<AB>> extends
    AbstractAggregationBuilder<AB> {

    public static final ParseField MULTIVALUE_MODE_FIELD = new ParseField("mode");

    public abstract static class LeafOnly<AB extends ArrayValuesSourceAggregationBuilder<AB>> extends ArrayValuesSourceAggregationBuilder<
        AB> {

        protected LeafOnly(String name) {
            super(name);
        }

        protected LeafOnly(LeafOnly<AB> clone, Builder factoriesBuilder, Map<String, Object> metadata) {
            super(clone, factoriesBuilder, metadata);
            if (factoriesBuilder.count() > 0) {
                throw new AggregationInitializationException(
                    "Aggregator [" + name + "] of type [" + getType() + "] cannot accept sub-aggregations"
                );
            }
        }

        /**
         * Read from a stream
         */
        protected LeafOnly(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public AB subAggregations(Builder subFactories) {
            throw new AggregationInitializationException(
                "Aggregator [" + name + "] of type [" + getType() + "] cannot accept sub-aggregations"
            );
        }

        @Override
        public final BucketCardinality bucketCardinality() {
            return BucketCardinality.NONE;
        }
    }

    private List<String> fields = Collections.emptyList();
    /* The parser doesn't support setting userValueTypeHint (aka valueType), but we do serialize and deserialize it, so keeping it around
    for now so as to not break BWC.  Future refactors should feel free to remove this field. --Tozzi 2020-01-16
     */
    private ValueType userValueTypeHint = null;
    private String format = null;
    private Object missing = null;
    private Map<String, Object> missingMap = Collections.emptyMap();

    protected ArrayValuesSourceAggregationBuilder(String name) {
        super(name);
    }

    protected ArrayValuesSourceAggregationBuilder(
        ArrayValuesSourceAggregationBuilder<AB> clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.fields = new ArrayList<>(clone.fields);
        this.userValueTypeHint = clone.userValueTypeHint;
        this.format = clone.format;
        this.missingMap = new HashMap<>(clone.missingMap);
        this.missing = clone.missing;
    }

    protected ArrayValuesSourceAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        read(in);
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    private void read(StreamInput in) throws IOException {
        fields = (ArrayList<String>) in.readGenericValue();
        userValueTypeHint = in.readOptionalWriteable(ValueType::readFromStream);
        format = in.readOptionalString();
        missingMap = in.readMap();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeGenericValue(fields);
        out.writeOptionalWriteable(userValueTypeHint);
        out.writeOptionalString(format);
        out.writeGenericMap(missingMap);
        innerWriteTo(out);
    }

    /**
     * Write subclass' state to the stream
     */
    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    /**
     * Sets the field to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB fields(List<String> fieldsArg) {
        if (fieldsArg == null) {
            throw new IllegalArgumentException("[field] must not be null: [" + name + "]");
        }
        this.fields = fieldsArg;
        return (AB) this;
    }

    /**
     * Gets the field to use for this aggregation.
     */
    public List<String> fields() {
        return fields;
    }

    /**
     * Sets the format to use for the output of the aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB format(String formatArg) {
        if (formatArg == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = formatArg;
        return (AB) this;
    }

    /**
     * Gets the format to use for the output of the aggregation.
     */
    public String format() {
        return format;
    }

    /**
     * Sets the value to use when the aggregation finds a missing value in a
     * document
     */
    @SuppressWarnings("unchecked")
    public AB missingMap(Map<String, Object> missingMapArg) {
        if (missingMapArg == null) {
            throw new IllegalArgumentException("[missing] must not be null: [" + name + "]");
        }
        this.missingMap = missingMapArg;
        return (AB) this;
    }

    /**
     * Gets the value to use when the aggregation finds a missing value in a
     * document
     */
    public Map<String, Object> missingMap() {
        return missingMap;
    }

    @Override
    protected final ArrayValuesSourceAggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        Builder subFactoriesBuilder
    ) throws IOException {
        Map<String, ValuesSourceConfig> configs = resolveConfig(context);
        ArrayValuesSourceAggregatorFactory factory = innerBuild(context, configs, parent, subFactoriesBuilder);
        return factory;
    }

    protected Map<String, ValuesSourceConfig> resolveConfig(AggregationContext context) {
        HashMap<String, ValuesSourceConfig> configs = new HashMap<>();
        for (String field : fields) {
            ValuesSourceConfig config = ValuesSourceConfig.resolveUnregistered(
                context,
                userValueTypeHint,
                field,
                null,
                missingMap.get(field),
                null,
                format,
                CoreValuesSourceType.KEYWORD
            );
            configs.put(field, config);
        }
        return configs;
    }

    protected abstract ArrayValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        Map<String, ValuesSourceConfig> configs,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException;

    @Override
    public final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // todo add ParseField support to XContentBuilder
        if (fields != null) {
            builder.field(CommonFields.FIELDS.getPreferredName(), fields);
        }
        if (missing != null) {
            builder.field(CommonFields.MISSING.getPreferredName(), missing);
        }
        if (format != null) {
            builder.field(CommonFields.FORMAT.getPreferredName(), format);
        }
        if (userValueTypeHint != null) {
            builder.field(CommonFields.VALUE_TYPE.getPreferredName(), userValueTypeHint.getPreferredName());
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields, format, missing, userValueTypeHint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ArrayValuesSourceAggregationBuilder<?> other = (ArrayValuesSourceAggregationBuilder<?>) obj;
        return Objects.equals(fields, other.fields)
            && Objects.equals(format, other.format)
            && Objects.equals(missing, other.missing)
            && Objects.equals(userValueTypeHint, other.userValueTypeHint);
    }
}
