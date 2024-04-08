/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;
import java.util.function.ToLongFunction;

/**
 * A {@link ValuesSource} builder for {@link CompositeAggregationBuilder}
 */
public abstract class CompositeValuesSourceBuilder<AB extends CompositeValuesSourceBuilder<AB>> implements Writeable, ToXContentFragment {

    protected final String name;
    private String field = null;
    private Script script = null;
    private ValueType userValueTypeHint = null;
    private boolean missingBucket = false;
    private MissingOrder missingOrder = MissingOrder.DEFAULT;
    private SortOrder order = SortOrder.ASC;
    private String format = null;

    CompositeValuesSourceBuilder(String name) {
        this.name = name;
    }

    CompositeValuesSourceBuilder(StreamInput in) throws IOException {
        this.name = in.readString();
        this.field = in.readOptionalString();
        if (in.readBoolean()) {
            this.script = new Script(in);
        }
        if (in.readBoolean()) {
            this.userValueTypeHint = ValueType.readFromStream(in);
        }
        this.missingBucket = in.readBoolean();
        this.missingOrder = MissingOrder.readFromStream(in);
        this.order = SortOrder.readFromStream(in);
        this.format = in.readOptionalString();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(field);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        boolean hasValueType = userValueTypeHint != null;
        out.writeBoolean(hasValueType);
        if (hasValueType) {
            userValueTypeHint.writeTo(out);
        }
        out.writeBoolean(missingBucket);
        missingOrder.writeTo(out);
        order.writeTo(out);
        out.writeOptionalString(format);
        innerWriteTo(out);
    }

    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    protected abstract void doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(type());
        if (field != null) {
            builder.field("field", field);
        }
        if (script != null) {
            builder.field("script", script);
        }
        builder.field("missing_bucket", missingBucket);
        if (missingOrder != MissingOrder.DEFAULT) {
            builder.field("missing_order", missingOrder.toString());
        }
        if (userValueTypeHint != null) {
            builder.field("value_type", userValueTypeHint.getPreferredName());
        }
        if (format != null) {
            builder.field("format", format);
        }
        builder.field("order", order);
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, missingBucket, missingOrder, script, userValueTypeHint, order, format);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        @SuppressWarnings("unchecked")
        AB that = (AB) o;
        return Objects.equals(field, that.field())
            && Objects.equals(script, that.script())
            && Objects.equals(userValueTypeHint, that.userValuetypeHint())
            && Objects.equals(missingBucket, that.missingBucket())
            && Objects.equals(missingOrder, that.missingOrder())
            && Objects.equals(order, that.order())
            && Objects.equals(format, that.format());
    }

    public String name() {
        return name;
    }

    abstract String type();

    /**
     * Sets the field to use for this source
     */
    @SuppressWarnings("unchecked")
    public AB field(String field) {
        if (field == null) {
            throw new IllegalArgumentException("[field] must not be null");
        }
        this.field = field;
        return (AB) this;
    }

    /**
     * Gets the field to use for this source
     */
    public String field() {
        return field;
    }

    /**
     * Sets the script to use for this source
     */
    @SuppressWarnings("unchecked")
    public AB script(Script script) {
        if (script == null) {
            throw new IllegalArgumentException("[script] must not be null");
        }
        this.script = script;
        return (AB) this;
    }

    /**
     * Gets the script to use for this source
     */
    public Script script() {
        return script;
    }

    /**
     * Sets the {@link ValueType} for the value produced by this source
     */
    @SuppressWarnings("unchecked")
    public AB userValuetypeHint(ValueType valueType) {
        if (valueType == null) {
            throw new IllegalArgumentException("[userValueTypeHint] must not be null");
        }
        this.userValueTypeHint = valueType;
        return (AB) this;
    }

    /**
     * Gets the {@link ValueType} for the value produced by this source
     */
    public ValueType userValuetypeHint() {
        return userValueTypeHint;
    }

    /**
     * If <code>true</code> an explicit <code>null</code> bucket will represent documents with missing values.
     */
    @SuppressWarnings("unchecked")
    public AB missingBucket(boolean missingBucket) {
        this.missingBucket = missingBucket;
        return (AB) this;
    }

    /**
     * False if documents with missing values are ignored, otherwise missing values are
     * represented by an explicit `null` value.
     */
    public boolean missingBucket() {
        return missingBucket;
    }

    /**
     * Sets the {@link MissingOrder} policy to use for ordering missing values.
     *
     * @param missingOrder One of "first", "last" or "default".
     */
    public AB missingOrder(String missingOrder) {
        return missingOrder(MissingOrder.fromString(missingOrder));
    }

    /**
     * Sets the {@link MissingOrder} policy to use for ordering missing values.
     */
    @SuppressWarnings("unchecked")
    public AB missingOrder(MissingOrder missingOrder) {
        if (missingOrder == null) {
            throw new IllegalArgumentException("[missingOrder] must not be null");
        }
        this.missingOrder = missingOrder;
        return (AB) this;
    }

    /**
     * The {@link MissingOrder} policy used for ordering missing values.
     */
    public MissingOrder missingOrder() {
        return missingOrder;
    }

    /**
     * Sets the {@link SortOrder} to use to sort values produced this source
     */
    @SuppressWarnings("unchecked")
    public AB order(String order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null");
        }
        this.order = SortOrder.fromString(order);
        return (AB) this;
    }

    /**
     * Sets the {@link SortOrder} to use to sort values produced this source
     */
    @SuppressWarnings("unchecked")
    public AB order(SortOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null");
        }
        this.order = order;
        return (AB) this;
    }

    /**
     * Gets the {@link SortOrder} to use to sort values produced this source
     */
    public SortOrder order() {
        return order;
    }

    /**
     * Sets the format to use for the output of the aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return (AB) this;
    }

    /**
     * Gets the format to use for the output of the aggregation.
     */
    public String format() {
        return format;
    }

    /**
     * Actually build the values source and its associated configuration.
     */
    protected abstract CompositeValuesSourceConfig innerBuild(ValuesSourceRegistry registry, ValuesSourceConfig config) throws IOException;

    protected abstract ValuesSourceType getDefaultValuesSourceType();

    public final CompositeValuesSourceConfig build(AggregationContext context) throws IOException {
        if (missingBucket == false && missingOrder != MissingOrder.DEFAULT) {
            throw new IllegalArgumentException("missingOrder can only be set if missingBucket is true");
        }

        ValuesSourceConfig config = ValuesSourceConfig.resolve(
            context,
            userValueTypeHint,
            field,
            script,
            null,
            timeZone(),
            format,
            getDefaultValuesSourceType()
        );
        return innerBuild(context.getValuesSourceRegistry(), config);
    }

    /**
     * The time zone for this value source. Default implementation returns {@code null}
     * because most value source types don't support time zone.
     */
    protected ZoneId timeZone() {
        return null;
    }

    /**
     * Return false if this composite source does not support parallel collection.
     * As a result, a request including such aggregation is always executed sequentially despite concurrency is enabled for the query phase.
     */
    public boolean supportsParallelCollection(ToLongFunction<String> fieldCardinalityResolver) {
        return true;
    }
}
