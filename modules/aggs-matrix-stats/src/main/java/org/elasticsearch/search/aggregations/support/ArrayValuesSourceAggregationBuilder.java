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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class ArrayValuesSourceAggregationBuilder<VS extends ValuesSource, AB extends ArrayValuesSourceAggregationBuilder<VS, AB>>
    extends AbstractAggregationBuilder<AB> {

    public static final ParseField MULTIVALUE_MODE_FIELD = new ParseField("mode");

    public abstract static class LeafOnly<VS extends ValuesSource, AB extends ArrayValuesSourceAggregationBuilder<VS, AB>>
        extends ArrayValuesSourceAggregationBuilder<VS, AB> {

        protected LeafOnly(String name, ValuesSourceType valuesSourceType, ValueType targetValueType) {
            super(name, valuesSourceType, targetValueType);
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
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" +
                getType() + "] cannot accept sub-aggregations");
        }
    }

    private final ValuesSourceType valuesSourceType;
    private final ValueType targetValueType;
    private List<String> fields = Collections.emptyList();
    private ValueType valueType = null;
    private String format = null;
    private Object missing = null;
    private Map<String, Object> missingMap = Collections.emptyMap();

    protected ArrayValuesSourceAggregationBuilder(String name, ValuesSourceType valuesSourceType, ValueType targetValueType) {
        super(name);
        if (valuesSourceType == null) {
            throw new IllegalArgumentException("[valuesSourceType] must not be null: [" + name + "]");
        }
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
    }

    protected ArrayValuesSourceAggregationBuilder(ArrayValuesSourceAggregationBuilder<VS, AB> clone,
                                                  Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.valuesSourceType = clone.valuesSourceType;
        this.targetValueType = clone.targetValueType;
        this.fields = new ArrayList<>(clone.fields);
        this.valueType = clone.valueType;
        this.format = clone.format;
        this.missingMap = new HashMap<>(clone.missingMap);
        this.missing = clone.missing;
    }

    protected ArrayValuesSourceAggregationBuilder(StreamInput in, ValuesSourceType valuesSourceType, ValueType targetValueType)
        throws IOException {
        super(in);
        assert false == serializeTargetValueType() : "Wrong read constructor called for subclass that provides its targetValueType";
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
        read(in);
    }

    protected ArrayValuesSourceAggregationBuilder(StreamInput in, ValuesSourceType valuesSourceType) throws IOException {
        super(in);
        assert serializeTargetValueType() : "Wrong read constructor called for subclass that serializes its targetValueType";
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = in.readOptionalWriteable(ValueType::readFromStream);
        read(in);
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    private void read(StreamInput in) throws IOException {
        fields = (ArrayList<String>)in.readGenericValue();
        valueType = in.readOptionalWriteable(ValueType::readFromStream);
        format = in.readOptionalString();
        missingMap = in.readMap();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        if (serializeTargetValueType()) {
            out.writeOptionalWriteable(targetValueType);
        }
        out.writeGenericValue(fields);
        out.writeOptionalWriteable(valueType);
        out.writeOptionalString(format);
        out.writeMap(missingMap);
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
    public AB missingMap(Map<String, Object> missingMap) {
        if (missingMap == null) {
            throw new IllegalArgumentException("[missing] must not be null: [" + name + "]");
        }
        this.missingMap = missingMap;
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
    protected final ArrayValuesSourceAggregatorFactory<VS> doBuild(SearchContext context, AggregatorFactory parent,
                                                                      AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        Map<String, ValuesSourceConfig<VS>> configs = resolveConfig(context);
        ArrayValuesSourceAggregatorFactory<VS> factory = innerBuild(context, configs, parent, subFactoriesBuilder);
        return factory;
    }

    protected Map<String, ValuesSourceConfig<VS>> resolveConfig(SearchContext context) {
        HashMap<String, ValuesSourceConfig<VS>> configs = new HashMap<>();
        for (String field : fields) {
            ValuesSourceConfig<VS> config = config(context, field, null);
            configs.put(field, config);
        }
        return configs;
    }

    protected abstract ArrayValuesSourceAggregatorFactory<VS> innerBuild(SearchContext context,
                                                                Map<String, ValuesSourceConfig<VS>> configs,
                                                                AggregatorFactory parent,
                                                                AggregatorFactories.Builder subFactoriesBuilder) throws IOException;

    public ValuesSourceConfig<VS> config(SearchContext context, String field, Script script) {

        ValueType valueType = this.valueType != null ? this.valueType : targetValueType;

        if (field == null) {
            if (script == null) {
                ValuesSourceConfig<VS> config = new ValuesSourceConfig<>(ValuesSourceType.ANY);
                return config.format(resolveFormat(null, valueType));
            }
            ValuesSourceType valuesSourceType = valueType != null ? valueType.getValuesSourceType() : this.valuesSourceType;
            if (valuesSourceType == null || valuesSourceType == ValuesSourceType.ANY) {
                // the specific value source type is undefined, but for scripts,
                // we need to have a specific value source
                // type to know how to handle the script values, so we fallback
                // on Bytes
                valuesSourceType = ValuesSourceType.BYTES;
            }
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<>(valuesSourceType);
            config.missing(missingMap.get(field));
            return config.format(resolveFormat(format, valueType));
        }

        MappedFieldType fieldType = context.smartNameFieldType(field);
        if (fieldType == null) {
            ValuesSourceType valuesSourceType = valueType != null ? valueType.getValuesSourceType() : this.valuesSourceType;
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<>(valuesSourceType);
            config.missing(missingMap.get(field));
            config.format(resolveFormat(format, valueType));
            return config.unmapped(true);
        }

        IndexFieldData<?> indexFieldData = context.getForField(fieldType);

        ValuesSourceConfig<VS> config;
        if (valuesSourceType == ValuesSourceType.ANY) {
            if (indexFieldData instanceof IndexNumericFieldData) {
                config = new ValuesSourceConfig<>(ValuesSourceType.NUMERIC);
            } else if (indexFieldData instanceof IndexGeoPointFieldData) {
                config = new ValuesSourceConfig<>(ValuesSourceType.GEOPOINT);
            } else {
                config = new ValuesSourceConfig<>(ValuesSourceType.BYTES);
            }
        } else {
            config = new ValuesSourceConfig<>(valuesSourceType);
        }

        config.fieldContext(new FieldContext(field, indexFieldData, fieldType));
        config.missing(missingMap.get(field));
        return config.format(fieldType.docValueFormat(format, null));
    }

    private static DocValueFormat resolveFormat(@Nullable String format, @Nullable ValueType valueType) {
        if (valueType == null) {
            return DocValueFormat.RAW; // we can't figure it out
        }
        DocValueFormat valueFormat = valueType.defaultFormat();
        if (valueFormat instanceof DocValueFormat.Decimal && format != null) {
            valueFormat = new DocValueFormat.Decimal(format);
        }
        return valueFormat;
    }

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
        return Objects.hash(super.hashCode(), fields, format, missing, targetValueType, valueType, valuesSourceType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ArrayValuesSourceAggregationBuilder<?, ?> other = (ArrayValuesSourceAggregationBuilder<?, ?>) obj;
        return Objects.equals(fields, other.fields)
            && Objects.equals(format, other.format)
            && Objects.equals(missing, other.missing)
            && Objects.equals(targetValueType, other.targetValueType)
            && Objects.equals(valueType, other.valueType)
            && Objects.equals(valuesSourceType, other.valuesSourceType);
    }
}
