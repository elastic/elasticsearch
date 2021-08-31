/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

/**
 * A {@link ValuesSource} builder for {@link CompositeAggregationBuilder}
 */
public abstract class CompositeValuesSourceBuilder<AB extends CompositeValuesSourceBuilder<AB>> implements Writeable, ToXContentFragment {

    protected final String name;
    private String field = null;
    private Script script = null;
    private ValueType userValueTypeHint = null;
    private MissingBucket missingBucket = MissingBucket.IGNORE;
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
        if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
            this.missingBucket = MissingBucket.readFromStream(in);
        } else {
            this.missingBucket = in.readBoolean() ? MissingBucket.INCLUDE : MissingBucket.IGNORE;
        }
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
        if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
            missingBucket.writeTo(out);
        } else {
            out.writeBoolean(missingBucket.include());
        }
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
        if (missingBucket == MissingBucket.IGNORE || missingBucket == MissingBucket.INCLUDE) {
            builder.field("missing_bucket", missingBucket.include());
        } else {
            builder.field("missing", missingBucket);
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
        return Objects.hash(field, missingBucket, script, userValueTypeHint, order, format);
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
            && Objects.equals(missingBucket, that.missing())
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
     *
     * @deprecated use `missing(String)` instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public AB missingBucket(boolean missingBucket) {
        this.missingBucket = missingBucket ? MissingBucket.INCLUDE : MissingBucket.IGNORE;
        return (AB) this;
    }

    /**
     * False if documents with missing values are ignored, otherwise missing values are
     * represented by an explicit `null` value.
     *
     * @deprecated Use `missing()` instead.
     */
    @Deprecated
    public boolean missingBucket() {
        return missingBucket.include();
    }

    /**
     * Sets the {@link MissingBucket} policy to use for handling documents with missing values.
     *
     * @param missing One of "_ignore", "_include", "_first" or "_last".
     */
    public AB missing(String missing) {
        return missing(MissingBucket.fromString(missing));
    }

    @SuppressWarnings("unchecked")
    public AB missing(MissingBucket missing) {
        this.missingBucket = missing;
        return (AB) this;
    }

    /**
     * The {@link MissingBucket} policy used for handling documents with missing values.
     */
    public MissingBucket missing() {
        return missingBucket;
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
}
