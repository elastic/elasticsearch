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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Similar to {@link ValuesSourceAggregationBuilder}, except it references multiple ValuesSources (e.g. so that an aggregation
 * can pull values from multiple fields).
 *
 * A limitation of this class is that all the ValuesSource's being refereenced must be of the same type.
 */
public abstract class MultiValuesSourceAggregationBuilder<VS extends ValuesSource, AB extends MultiValuesSourceAggregationBuilder<VS, AB>>
        extends AbstractAggregationBuilder<AB> {


    public abstract static class LeafOnly<VS extends ValuesSource, AB extends MultiValuesSourceAggregationBuilder<VS, AB>>
            extends MultiValuesSourceAggregationBuilder<VS, AB> {

        protected LeafOnly(String name, ValueType targetValueType) {
            super(name, targetValueType);
        }

        protected LeafOnly(LeafOnly<VS, AB> clone, Builder factoriesBuilder, Map<String, Object> metaData) {
            super(clone, factoriesBuilder, metaData);
            if (factoriesBuilder.count() > 0) {
                throw new AggregationInitializationException("Aggregator [" + name + "] of type ["
                    + getType() + "] cannot accept sub-aggregations");
            }
        }

        /**
         * Read from a stream that does not serialize its targetValueType. This should be used by most subclasses.
         */
        protected LeafOnly(StreamInput in, ValueType targetValueType) throws IOException {
            super(in, targetValueType);
        }

        @Override
        public AB subAggregations(Builder subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" +
                getType() + "] cannot accept sub-aggregations");
        }
    }



    private Map<String, MultiValuesSourceFieldConfig> fields = new HashMap<>();
    private final ValueType targetValueType;
    private ValueType valueType = null;
    private String format = null;

    protected MultiValuesSourceAggregationBuilder(String name, ValueType targetValueType) {
        super(name);
        this.targetValueType = targetValueType;
    }

    protected MultiValuesSourceAggregationBuilder(MultiValuesSourceAggregationBuilder<VS, AB> clone,
                                                  Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);

        this.fields = new HashMap<>(clone.fields);
        this.targetValueType = clone.targetValueType;
        this.valueType = clone.valueType;
        this.format = clone.format;
    }

    protected MultiValuesSourceAggregationBuilder(StreamInput in, ValueType targetValueType)
        throws IOException {
        super(in);
        assert false == serializeTargetValueType() : "Wrong read constructor called for subclass that provides its targetValueType";
        this.targetValueType = targetValueType;
        read(in);
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    private void read(StreamInput in) throws IOException {
        fields = in.readMap(StreamInput::readString, MultiValuesSourceFieldConfig::new);
        valueType = in.readOptionalWriteable(ValueType::readFromStream);
        format = in.readOptionalString();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        if (serializeTargetValueType()) {
            out.writeOptionalWriteable(targetValueType);
        }
        out.writeMap(fields, StreamOutput::writeString, (o, value) -> value.writeTo(o));
        out.writeOptionalWriteable(valueType);
        out.writeOptionalString(format);
        innerWriteTo(out);
    }

    /**
     * Write subclass' state to the stream
     */
    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    @SuppressWarnings("unchecked")
    protected AB field(String propertyName, MultiValuesSourceFieldConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("[config] must not be null: [" + name + "]");
        }
        this.fields.put(propertyName, config);
        return (AB) this;
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

    @Override
    protected final MultiValuesSourceAggregatorFactory<VS> doBuild(QueryShardContext queryShardContext, AggregatorFactory parent,
                                                                   Builder subFactoriesBuilder) throws IOException {
        ValueType finalValueType = this.valueType != null ? this.valueType : targetValueType;

        Map<String, ValuesSourceConfig<VS>> configs = new HashMap<>(fields.size());
        fields.forEach((key, value) -> {
            ValuesSourceConfig<VS> config = ValuesSourceConfig.resolve(queryShardContext, finalValueType,
                value.getFieldName(), value.getScript(), value.getMissing(), value.getTimeZone(), format);
            configs.put(key, config);
        });
        DocValueFormat docValueFormat = resolveFormat(format, finalValueType);
        return innerBuild(queryShardContext, configs, docValueFormat, parent, subFactoriesBuilder);
    }


    private static DocValueFormat resolveFormat(@Nullable String format, @Nullable ValueType valueType) {
        if (valueType == null) {
            return DocValueFormat.RAW; // we can't figure it out
        }
        DocValueFormat valueFormat = valueType.defaultFormat;
        if (valueFormat instanceof DocValueFormat.Decimal && format != null) {
            valueFormat = new DocValueFormat.Decimal(format);
        }
        return valueFormat;
    }

    protected abstract MultiValuesSourceAggregatorFactory<VS> innerBuild(QueryShardContext queryShardContext,
                                                                         Map<String, ValuesSourceConfig<VS>> configs,
                                                                         DocValueFormat format, AggregatorFactory parent,
                                                                         Builder subFactoriesBuilder) throws IOException;


    /**
     * Should this builder serialize its targetValueType? Defaults to false. All subclasses that override this to true
     * should use the three argument read constructor rather than the four argument version.
     */
    protected boolean serializeTargetValueType() {
        return false;
    }

    @Override
    public final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (fields != null) {
            for (Map.Entry<String, MultiValuesSourceFieldConfig> fieldEntry : fields.entrySet()) {
                builder.field(fieldEntry.getKey(), fieldEntry.getValue());
            }
        }
        if (format != null) {
            builder.field(CommonFields.FORMAT.getPreferredName(), format);
        }
        if (valueType != null) {
            builder.field(CommonFields.VALUE_TYPE.getPreferredName(), valueType.getPreferredName());
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields, format, targetValueType, valueType);
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        MultiValuesSourceAggregationBuilder other = (MultiValuesSourceAggregationBuilder) obj;
        return Objects.equals(this.fields, other.fields)
            && Objects.equals(this.format, other.format)
            && Objects.equals(this.valueType, other.valueType);
    }
}
