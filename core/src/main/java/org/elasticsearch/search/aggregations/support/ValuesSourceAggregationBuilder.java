/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;

public abstract class ValuesSourceAggregationBuilder<VS extends ValuesSource, AB extends ValuesSourceAggregationBuilder<VS, AB>>
        extends AbstractAggregationBuilder<AB> {

    public abstract static class LeafOnly<VS extends ValuesSource, AB extends ValuesSourceAggregationBuilder<VS, AB>>
            extends ValuesSourceAggregationBuilder<VS, AB> {

        protected LeafOnly(String name, ValuesSourceType valuesSourceType, ValueType targetValueType) {
            super(name, valuesSourceType, targetValueType);
        }

        /**
         * Read an aggregation from a stream that does not serialize its targetValueType. This should be used by most subclasses.
         */
        protected LeafOnly(StreamInput in, ValuesSourceType valuesSourceType, ValueType targetValueType) throws IOException {
            super(in, valuesSourceType, targetValueType);
        }

        /**
         * Read an aggregation from a stream that serializes its targetValueType. This should only be used by subclasses that override
         * {@link #serializeTargetValueType()} to return true.
         */
        protected LeafOnly(StreamInput in, ValuesSourceType valuesSourceType) throws IOException {
            super(in, valuesSourceType);
        }

        @Override
        public AB subAggregations(Builder subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type ["
                    + getType() + "] cannot accept sub-aggregations");
        }
    }

    private final ValuesSourceType valuesSourceType;
    private final ValueType targetValueType;
    private String field = null;
    private Script script = null;
    private ValueType valueType = null;
    private String format = null;
    private Object missing = null;
    private DateTimeZone timeZone = null;
    protected ValuesSourceConfig<VS> config;

    protected ValuesSourceAggregationBuilder(String name, ValuesSourceType valuesSourceType, ValueType targetValueType) {
        super(name);
        if (valuesSourceType == null) {
            throw new IllegalArgumentException("[valuesSourceType] must not be null: [" + name + "]");
        }
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
    }

    /**
     * Read an aggregation from a stream that does not serialize its targetValueType. This should be used by most subclasses.
     */
    protected ValuesSourceAggregationBuilder(StreamInput in, ValuesSourceType valuesSourceType, ValueType targetValueType)
            throws IOException {
        super(in);
        assert false == serializeTargetValueType() : "Wrong read constructor called for subclass that provides its targetValueType";
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
        read(in);
    }

    /**
     * Read an aggregation from a stream that serializes its targetValueType. This should only be used by subclasses that override
     * {@link #serializeTargetValueType()} to return true.
     */
    protected ValuesSourceAggregationBuilder(StreamInput in, ValuesSourceType valuesSourceType) throws IOException {
        super(in);
        assert serializeTargetValueType() : "Wrong read constructor called for subclass that serializes its targetValueType";
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = in.readOptionalWriteable(ValueType::readFromStream);
        read(in);
    }

    /**
     * Read from a stream.
     */
    private void read(StreamInput in) throws IOException {
        field = in.readOptionalString();
        if (in.readBoolean()) {
            script = new Script(in);
        }
        if (in.readBoolean()) {
            valueType = ValueType.readFromStream(in);
        }
        format = in.readOptionalString();
        missing = in.readGenericValue();
        if (in.readBoolean()) {
            timeZone = DateTimeZone.forID(in.readString());
        }
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        if (serializeTargetValueType()) {
            out.writeOptionalWriteable(targetValueType);
        }
        out.writeOptionalString(field);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        boolean hasValueType = valueType != null;
        out.writeBoolean(hasValueType);
        if (hasValueType) {
            valueType.writeTo(out);
        }
        out.writeOptionalString(format);
        out.writeGenericValue(missing);
        boolean hasTimeZone = timeZone != null;
        out.writeBoolean(hasTimeZone);
        if (hasTimeZone) {
            out.writeString(timeZone.getID());
        }
        innerWriteTo(out);
    }

    /**
     * Write subclass's state to the stream.
     */
    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    /**
     * Should this builder serialize its targetValueType? Defaults to false. All subclasses that override this to true should use the three
     * argument read constructor rather than the four argument version.
     */
    protected boolean serializeTargetValueType() {
        return false;
    }

    /**
     * Sets the field to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB field(String field) {
        if (field == null) {
            throw new IllegalArgumentException("[field] must not be null: [" + name + "]");
        }
        this.field = field;
        return (AB) this;
    }

    /**
     * Gets the field to use for this aggregation.
     */
    public String field() {
        return field;
    }

    /**
     * Sets the script to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB script(Script script) {
        if (script == null) {
            throw new IllegalArgumentException("[script] must not be null: [" + name + "]");
        }
        this.script = script;
        return (AB) this;
    }

    /**
     * Gets the script to use for this aggregation.
     */
    public Script script() {
        return script;
    }

    /**
     * Sets the {@link ValueType} for the value produced by this aggregation
     */
    @SuppressWarnings("unchecked")
    public AB valueType(ValueType valueType) {
        if (valueType == null) {
            throw new IllegalArgumentException("[valueType] must not be null: [" + name + "]");
        }
        this.valueType = valueType;
        return (AB) this;
    }

    /**
     * Gets the {@link ValueType} for the value produced by this aggregation
     */
    public ValueType valueType() {
        return valueType;
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
     * Sets the value to use when the aggregation finds a missing value in a
     * document
     */
    @SuppressWarnings("unchecked")
    public AB missing(Object missing) {
        if (missing == null) {
            throw new IllegalArgumentException("[missing] must not be null: [" + name + "]");
        }
        this.missing = missing;
        return (AB) this;
    }

    /**
     * Gets the value to use when the aggregation finds a missing value in a
     * document
     */
    public Object missing() {
        return missing;
    }

    /**
     * Sets the time zone to use for this aggregation
     */
    @SuppressWarnings("unchecked")
    public AB timeZone(DateTimeZone timeZone) {
        if (timeZone == null) {
            throw new IllegalArgumentException("[timeZone] must not be null: [" + name + "]");
        }
        this.timeZone = timeZone;
        return (AB) this;
    }

    /**
     * Gets the time zone to use for this aggregation
     */
    public DateTimeZone timeZone() {
        return timeZone;
    }

    @Override
    protected final ValuesSourceAggregatorFactory<VS, ?> doBuild(SearchContext context, AggregatorFactory<?> parent,
            AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        ValuesSourceConfig<VS> config = resolveConfig(context);
        ValuesSourceAggregatorFactory<VS, ?> factory = innerBuild(context, config, parent, subFactoriesBuilder);
        return factory;
    }

    protected ValuesSourceConfig<VS> resolveConfig(SearchContext context) {
        ValueType valueType = this.valueType != null ? this.valueType : targetValueType;
        return ValuesSourceConfig.resolve(context.getQueryShardContext(),
                valueType, field, script, missing, timeZone, format);
    }

    protected abstract ValuesSourceAggregatorFactory<VS, ?> innerBuild(SearchContext context, ValuesSourceConfig<VS> config,
            AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException;

    @Override
    public final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (script != null) {
            builder.field("script", script);
        }
        if (missing != null) {
            builder.field("missing", missing);
        }
        if (format != null) {
            builder.field("format", format);
        }
        if (timeZone != null) {
            builder.field("time_zone", timeZone);
        }
        if (valueType != null) {
            builder.field("value_type", valueType.getPreferredName());
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    protected final int doHashCode() {
        return Objects.hash(field, format, missing, script, targetValueType, timeZone, valueType, valuesSourceType,
                innerHashCode());
    }

    protected abstract int innerHashCode();

    @Override
    protected final boolean doEquals(Object obj) {
        ValuesSourceAggregationBuilder<?, ?> other = (ValuesSourceAggregationBuilder<?, ?>) obj;
        if (!Objects.equals(field, other.field))
            return false;
        if (!Objects.equals(format, other.format))
            return false;
        if (!Objects.equals(missing, other.missing))
            return false;
        if (!Objects.equals(script, other.script))
            return false;
        if (!Objects.equals(targetValueType, other.targetValueType))
            return false;
        if (!Objects.equals(timeZone, other.timeZone))
            return false;
        if (!Objects.equals(valueType, other.valueType))
            return false;
        if (!Objects.equals(valuesSourceType, other.valuesSourceType))
            return false;
        return innerEquals(obj);
    }

    protected abstract boolean innerEquals(Object obj);
}
