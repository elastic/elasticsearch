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
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public abstract class MultiValuesSourceAggregatorBuilder<VS extends ValuesSource, AB extends MultiValuesSourceAggregatorBuilder<VS, AB>>
    extends ValuesSourceAggregatorBuilder<VS, AB> {

    public static abstract class LeafOnly<VS extends ValuesSource, AB extends MultiValuesSourceAggregatorBuilder<VS, AB>>
        extends MultiValuesSourceAggregatorBuilder<VS, AB> {

        protected LeafOnly(String name, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType) {
            super(name, type, valuesSourceType, targetValueType);
        }

        /**
         * Read from a stream that does not serialize its targetValueType. This should be used by most subclasses.
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
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" +
                type + "] cannot accept sub-aggregations");
        }
    }

    private List<String> fields;
    private Map<String, Script> scripts;

    protected MultiValuesSourceAggregatorBuilder(String name, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType) {
        super(name, type, valuesSourceType, targetValueType);
        if (fields == null) {
            this.fields = Collections.emptyList();
        }
        if (scripts == null) {
            this.scripts = Collections.emptyMap();
        }
    }

    protected MultiValuesSourceAggregatorBuilder(StreamInput in, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType)
        throws IOException {
        super(in, type, valuesSourceType, targetValueType);
    }

    protected MultiValuesSourceAggregatorBuilder(StreamInput in, Type type, ValuesSourceType valuesSourceType) throws IOException {
        super(in, type, valuesSourceType);
    }

    /**
     * Read from a stream.
     */
    @Override
    protected void read(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            int size = in.readVInt();
            fields = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                fields.add(in.readString());
            }
        } else {
            fields = Collections.emptyList();
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            scripts = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                scripts.put(in.readString(), new Script(in));
            }
        } else {
            scripts = Collections.emptyMap();
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
        boolean hasFields = fields != null;
        out.writeBoolean(hasFields);
        if (hasFields) {
            out.writeVInt(fields.size());
            for (String field : fields) {
                out.writeString(field);
            }
        }
        boolean hasScripts = scripts != null;
        out.writeBoolean(hasScripts);
        if (hasScripts) {
            out.writeVInt(scripts.size());
            for (Map.Entry<String, Script> script : scripts.entrySet()) {
                out.writeString(script.getKey());
                script.getValue().writeTo(out);
            }
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
     * Sets the field to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB fields(List<String> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("[field] must not be null: [" + name + "]");
        }
        this.fields = fields;
        return (AB) this;
    }

    /**
     * Gets the field to use for this aggregation.
     */
    public List<String> fields() {
        return fields;
    }

    /**
     * Sets the script to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB scripts(Map<String, Script> scripts) {
        if (scripts == null) {
            throw new IllegalArgumentException("[script] must not be null: [" + name + "]");
        }
        this.scripts = scripts;
        return (AB) this;
    }

    /**
     * Gets the script to use for this aggregation.
     */
    public Map<String, Script> scripts() {
        return scripts;
    }

    @Override
    protected final MultiValuesSourceAggregatorFactory<VS, ?> doBuild(AggregationContext context, AggregatorFactory<?> parent,
            AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        Map<String, ValuesSourceConfig<VS>> configs = resolveConfig(context);
        MultiValuesSourceAggregatorFactory<VS, ?> factory = innerBuild(context, configs, parent, subFactoriesBuilder);
        return factory;
    }

    protected Map<String, ValuesSourceConfig<VS>> resolveConfig(AggregationContext context) {
        HashMap<String, ValuesSourceConfig<VS>> configs = new HashMap<>();
        for (String field : fields) {
            ValuesSourceConfig<VS> config = config(context, field, null);
            configs.put(field, config);
        }
        for (Map.Entry<String, Script> script : scripts.entrySet()) {
            ValuesSourceConfig<VS> config = config(context, null, script.getValue());
            configs.put(script.getKey(), config);
        }
        return configs;
    }

    protected abstract MultiValuesSourceAggregatorFactory<VS, ?> innerBuild(AggregationContext context,
            Map<String, ValuesSourceConfig<VS>> configs, AggregatorFactory<?> parent,
            AggregatorFactories.Builder subFactoriesBuilder) throws IOException;

    @Override
    public final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (fields != null) {
            builder.field("field", fields);
        }
        if (scripts != null) {
            builder.field("script", scripts);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected final int doHashCode() {
        return Objects.hash(super.doHashCode(), fields, scripts);
    }

    @Override
    protected final boolean doEquals(Object obj) {
        MultiValuesSourceAggregatorBuilder<?, ?> other = (MultiValuesSourceAggregatorBuilder<?, ?>) obj;
        if (!Objects.equals(fields, other.fields))
            return false;
        if (!Objects.equals(scripts, other.scripts))
            return false;
        return super.doEquals(obj);
    }
}
