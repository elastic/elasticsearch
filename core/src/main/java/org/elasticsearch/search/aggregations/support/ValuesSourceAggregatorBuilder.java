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
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 *
 */
public abstract class ValuesSourceAggregatorBuilder<VS extends ValuesSource, AB extends ValuesSourceAggregatorBuilder<VS, AB>>
    extends AggregatorBuilder<AB> {
    protected final ValuesSourceType valuesSourceType;
    protected final ValueType targetValueType;
    protected ValueType valueType = null;
    protected String format = null;
    protected Object missing = null;
    protected DateTimeZone timeZone = null;
    protected ValuesSourceConfig<VS> config;

    protected ValuesSourceAggregatorBuilder(String name, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType) {
        super(name, type);
        if (valuesSourceType == null) {
            throw new IllegalArgumentException("[valuesSourceType] must not be null: [" + name + "]");
        }
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
    }

    /**
     * Read an aggregation from a stream that does not serialize its targetValueType. This should be used by most subclasses.
     */
    protected ValuesSourceAggregatorBuilder(StreamInput in, Type type, ValuesSourceType valuesSourceType, ValueType targetValueType)
            throws IOException {
        super(in, type);
        assert false == serializeTargetValueType() : "Wrong read constructor called for subclass that provides its targetValueType";
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
        read(in);
    }

    /**
     * Read an aggregation from a stream that serializes its targetValueType. This should only be used by subclasses that override
     * {@link #serializeTargetValueType()} to return true.
     */
    protected ValuesSourceAggregatorBuilder(StreamInput in, Type type, ValuesSourceType valuesSourceType) throws IOException {
        super(in, type);
        assert serializeTargetValueType() : "Wrong read constructor called for subclass that serializes its targetValueType";
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = in.readOptionalWriteable(ValueType::readFromStream);
        read(in);
    }

    protected abstract void read(StreamInput in) throws IOException;

    /**
     * Write subclass's state to the stream.
     */
    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    /**
     * Should this builder serialize its targetValueType? Defaults to false. All subclasses that override this to true
     * should use the three argument read constructor rather than the four argument version.
     */
    protected boolean serializeTargetValueType() {
        return false;
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

    protected ValuesSourceConfig<VS> config(AggregationContext context, String field, Script script) {

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
        config.format = fieldType.docValueFormat(format, timeZone);
        return config;
    }

    private SearchScript createScript(Script script, SearchContext context) {
        return script == null ? null
                : context.scriptService().search(context.lookup(), script, ScriptContext.Standard.AGGS, Collections.emptyMap(),
                context.getQueryShardContext().getClusterState());
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

    @Override
    public XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
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
    protected int doHashCode() {
        return Objects.hash(format, missing, targetValueType, timeZone, valueType, valuesSourceType,
            innerHashCode());
    }

    @Override
    protected boolean doEquals(Object obj) {
        ValuesSourceAggregatorBuilder<?, ?> other = (ValuesSourceAggregatorBuilder<?, ?>) obj;
        if (!Objects.equals(format, other.format))
            return false;
        if (!Objects.equals(missing, other.missing))
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

    protected abstract int innerHashCode();
    protected abstract boolean innerEquals(Object obj);
}
