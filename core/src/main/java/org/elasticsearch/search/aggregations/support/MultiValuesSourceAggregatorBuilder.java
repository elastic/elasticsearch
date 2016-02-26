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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public abstract class MultiValuesSourceAggregatorBuilder<VS extends ValuesSource, AB extends MultiValuesSourceAggregatorBuilder<VS, AB>>
        extends AggregatorBuilder<AB> {

    public static abstract class LeafOnly<VS extends ValuesSource, AB extends MultiValuesSourceAggregatorBuilder<VS, AB>>
            extends MultiValuesSourceAggregatorBuilder<VS, AB> {

        protected LeafOnly(String name, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType) {
            super(name, type, valuesSourceType, targetValueType);
        }

        @Override
        public AB subAggregations(Builder subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }
    }

    private final ValuesSourceType valuesSourceType;
    private final ValueType targetValueType;
    private List<String> fields = Collections.emptyList();
    private List<Script> scripts = Collections.emptyList();
    private ValueType valueType = null;
    private String format = null;
    private Object missing = null;
    private DateTimeZone timeZone;

    protected MultiValuesSourceAggregatorBuilder(String name, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType) {
        super(name, type);
        if (valuesSourceType == null) {
            throw new IllegalArgumentException("[valuesSourceType] must not be null: [" + name + "]");
        }
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
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
    public AB scripts(List<Script> scripts) {
        if (scripts == null) {
            throw new IllegalArgumentException("[script] must not be null: [" + name + "]");
        }
        this.scripts = scripts;
        return (AB) this;
    }

    /**
     * Gets the script to use for this aggregation.
     */
    public List<Script> scripts() {
        return scripts;
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
        for (Script script : scripts) {
            ValuesSourceConfig<VS> config = config(context, null, script);
            configs.put(script.getType().getParseField().getPreferredName(), config);
        }
        return configs;
    }

    protected abstract MultiValuesSourceAggregatorFactory<VS, ?> innerBuild(AggregationContext context,
                                                                            Map<String, ValuesSourceConfig<VS>> configs,
                                                                            AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException;

    public ValuesSourceConfig<VS> config(AggregationContext context, String field, Script script) {

        ValueType valueType = this.valueType != null ? this.valueType : targetValueType;

        if (field == null) {
            if (script == null) {
                @SuppressWarnings("unchecked")
                ValuesSourceConfig<VS> config = new ValuesSourceConfig(ValuesSourceType.ANY);
                config.format = resolveFormat(null, valueType);
                return config;
            }
            ValuesSourceType valuesSourceType = valueType != null ? valueType.getValuesSourceType() : this.valuesSourceType;
            if (valuesSourceType == null || valuesSourceType == ValuesSourceType.ANY) {
                // the specific value source type is undefined, but for scripts,
                // we need to have a specific value source
                // type to know how to handle the script values, so we fallback
                // on Bytes
                valuesSourceType = ValuesSourceType.BYTES;
            }
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<VS>(valuesSourceType);
            config.missing = missing;
            config.timeZone = timeZone;
            config.format = resolveFormat(format, valueType);
            config.script = createScript(script, context.searchContext());
            config.scriptValueType = valueType;
            return config;
        }

        MappedFieldType fieldType = context.searchContext().smartNameFieldType(field);
        if (fieldType == null) {
            ValuesSourceType valuesSourceType = valueType != null ? valueType.getValuesSourceType() : this.valuesSourceType;
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<>(valuesSourceType);
            config.missing = missing;
            config.timeZone = timeZone;
            config.format = resolveFormat(format, valueType);
            config.unmapped = true;
            if (valueType != null) {
                // todo do we really need this for unmapped?
                config.scriptValueType = valueType;
            }
            return config;
        }

        IndexFieldData<?> indexFieldData = context.searchContext().fieldData().getForField(fieldType);

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
            config = new ValuesSourceConfig(valuesSourceType);
        }

        config.fieldContext = new FieldContext(field, indexFieldData, fieldType);
        config.missing = missing;
        config.timeZone = timeZone;
        config.script = createScript(script, context.searchContext());
        config.format = resolveFormat(format, this.timeZone, fieldType);
        return config;
    }

    private SearchScript createScript(Script script, SearchContext context) {
        return script == null ? null
                : context.scriptService().search(context.lookup(), script, ScriptContext.Standard.AGGS, Collections.emptyMap());
    }

    private static ValueFormat resolveFormat(@Nullable String format, @Nullable ValueType valueType) {
        if (valueType == null) {
            return ValueFormat.RAW; // we can't figure it out
        }
        ValueFormat valueFormat = valueType.defaultFormat;
        if (valueFormat != null && valueFormat instanceof ValueFormat.Patternable && format != null) {
            return ((ValueFormat.Patternable) valueFormat).create(format);
        }
        return valueFormat;
    }

    private static ValueFormat resolveFormat(@Nullable String format, @Nullable DateTimeZone timezone, MappedFieldType fieldType) {
        if (fieldType instanceof DateFieldMapper.DateFieldType) {
            return format != null ? ValueFormat.DateTime.format(format, timezone) : ValueFormat.DateTime.mapper(
                    (DateFieldMapper.DateFieldType) fieldType, timezone);
        }
        if (fieldType instanceof IpFieldMapper.IpFieldType) {
            return ValueFormat.IPv4;
        }
        if (fieldType instanceof BooleanFieldMapper.BooleanFieldType) {
            return ValueFormat.BOOLEAN;
        }
        if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            return format != null ? ValueFormat.Number.format(format) : ValueFormat.RAW;
        }
        return ValueFormat.RAW;
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        valuesSourceType.writeTo(out);
        boolean hasTargetValueType = targetValueType != null;
        out.writeBoolean(hasTargetValueType);
        if (hasTargetValueType) {
            targetValueType.writeTo(out);
        }
        innerWriteTo(out);
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
            for (Script script : scripts) {
                script.writeTo(out);
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
    }

    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    @SuppressWarnings("unchecked")
    @Override
    protected final AB doReadFrom(String name, StreamInput in) throws IOException {
        ValuesSourceType valuesSourceType = ValuesSourceType.ANY.readFrom(in);
        ValueType targetValueType = null;
        if (in.readBoolean()) {
            targetValueType = ValueType.STRING.readFrom(in);
        }
        MultiValuesSourceAggregatorBuilder<VS, AB> factory = innerReadFrom(name, valuesSourceType, targetValueType, in);
        if (in.readBoolean()) {
            int size = in.readVInt();
            List<String> fields = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                fields.add(in.readString());
            }
            factory.fields = fields;
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            List<Script> scripts = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                scripts.add(Script.readScript(in));
            }
            factory.scripts = scripts;
        }
        if (in.readBoolean()) {
            factory.valueType = ValueType.STRING.readFrom(in);
        }
        factory.format = in.readOptionalString();
        factory.missing = in.readGenericValue();
        if (in.readBoolean()) {
            factory.timeZone = DateTimeZone.forID(in.readString());
        }
        return (AB) factory;
    }

    protected abstract MultiValuesSourceAggregatorBuilder<VS, AB> innerReadFrom(String name, ValuesSourceType valuesSourceType,
                                                                                ValueType targetValueType, StreamInput in) throws IOException;

    @Override
    public final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (fields != null) {
            builder.field("field", fields);
        }
        if (scripts != null) {
            builder.field("script", scripts);
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
        return Objects.hash(fields, format, missing, scripts, targetValueType, timeZone, valueType, valuesSourceType,
                innerHashCode());
    }

    protected abstract int innerHashCode();

    @Override
    protected final boolean doEquals(Object obj) {
        MultiValuesSourceAggregatorBuilder<?, ?> other = (MultiValuesSourceAggregatorBuilder<?, ?>) obj;
        if (!Objects.equals(fields, other.fields))
            return false;
        if (!Objects.equals(format, other.format))
            return false;
        if (!Objects.equals(missing, other.missing))
            return false;
        if (!Objects.equals(scripts, other.scripts))
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
