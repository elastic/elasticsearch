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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public abstract class SingleValuesSourceAggregatorBuilder<VS extends ValuesSource, AB extends SingleValuesSourceAggregatorBuilder<VS, AB>>
    extends ValuesSourceAggregatorBuilder<VS, AB> {

    public static abstract class LeafOnly<VS extends ValuesSource, AB extends SingleValuesSourceAggregatorBuilder<VS, AB>>
        extends SingleValuesSourceAggregatorBuilder<VS, AB> {

        protected LeafOnly(String name, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType) {
            super(name, type, valuesSourceType, targetValueType);
        }

        /**
         * Read an aggregation from a stream that does not serialize its targetValueType. This should be used by most subclasses.
         */
        protected LeafOnly(StreamInput in, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType) throws IOException {
            super(in, type, valuesSourceType, targetValueType);
        }

        /**
         * Read an aggregation from a stream that serializes its targetValueType. This should only be used by subclasses that override
         * {@link #serializeTargetValueType()} to return true.
         */
        protected LeafOnly(StreamInput in, Type type, ValuesSourceType valuesSourceType) throws IOException {
            super(in, type, valuesSourceType);
        }

        @Override
        public AB subAggregations(Builder subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }
    }

    private String field;
    private Script script;

    protected SingleValuesSourceAggregatorBuilder(String name, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType) {
        super(name, type, valuesSourceType, targetValueType);
    }

    /**
     * Read an aggregation from a stream that does not serialize its targetValueType. This should be used by most subclasses.
     */
    protected SingleValuesSourceAggregatorBuilder(StreamInput in, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType)
        throws IOException {
        super(in, type, valuesSourceType, targetValueType);
    }

    /**
     * Read an aggregation from a stream that serializes its targetValueType. This should only be used by subclasses that override
     * {@link #serializeTargetValueType()} to return true.
     */
    protected SingleValuesSourceAggregatorBuilder(StreamInput in, Type type, ValuesSourceType valuesSourceType) throws IOException {
        super(in, type, valuesSourceType);
    }

    /**
     * Read from a stream.
     */
    @Override
    protected void read(StreamInput in) throws IOException {
        super.read(in);
        field = in.readOptionalString();
        if (in.readBoolean()) {
            script = new Script(in);
        }
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeOptionalString(field);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        innerWriteTo(out);
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

    @Override
    protected SingleValuesSourceAggregatorFactory<VS, ?> doBuild(AggregationContext context, AggregatorFactory<?> parent,
            AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        ValuesSourceConfig<VS> config = resolveConfig(context);
        SingleValuesSourceAggregatorFactory<VS, ?> factory = innerBuild(context, config, parent, subFactoriesBuilder);
        return factory;
    }

    protected ValuesSourceConfig<VS> resolveConfig(AggregationContext context) {
        ValuesSourceConfig<VS> config = config(context, field, script);
        return config;
    }

    protected abstract SingleValuesSourceAggregatorFactory<VS, ?> innerBuild(AggregationContext context, ValuesSourceConfig<VS> config,
            AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException;

    @Override
    public XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field(ParseField.FIELD_FIELD.getPreferredName(), field);
        }
        if (script != null) {
            builder.field(Script.ScriptField.SCRIPT.getPreferredName(), script);
        }
        return super.internalXContent(builder, params);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode(), field, script);
    }

    @Override
    protected boolean doEquals(Object obj) {
        SingleValuesSourceAggregatorBuilder<?, ?> other = (SingleValuesSourceAggregatorBuilder<?, ?>) obj;
        if (!Objects.equals(field, other.field))
            return false;
        if (!Objects.equals(script, other.script))
            return false;
        return super.doEquals(obj);
    }
}
