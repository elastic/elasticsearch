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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.function.Function;

/**
 * A configuration that tells aggregations how to retrieve data from the index
 * in order to run a specific aggregation.
 */
public class ValuesSourceConfig<VS extends ValuesSource> {

    /**
     * Resolve a {@link ValuesSourceConfig} given configuration parameters.
     */
    public static <VS extends ValuesSource> ValuesSourceConfig<VS> resolve(
        QueryShardContext context,
        ValueType valueType,
        String field, Script script,
        Object missing,
        ZoneId timeZone,
        String format) {
        return resolve(context, valueType, field, script, missing, timeZone, format, s -> CoreValuesSourceType.BYTES);
    }

    /**
     * Resolve a {@link ValuesSourceConfig} given configuration parameters.
     */
    public static <VS extends ValuesSource> ValuesSourceConfig<VS> resolve(
        QueryShardContext context,
        ValueType valueType,
        String field, Script script,
        Object missing,
        ZoneId timeZone,
        String format,
        Function<Script, ValuesSourceType> resolveScriptAny
    ) {

        if (field == null) {
            if (script == null) {
                ValuesSourceConfig<VS> config = new ValuesSourceConfig<>(CoreValuesSourceType.ANY);
                config.format(resolveFormat(null, valueType, timeZone));
                return config;
            }
            ValuesSourceType valuesSourceType = valueType != null ? valueType.getValuesSourceType() : CoreValuesSourceType.ANY;
            if (valuesSourceType == CoreValuesSourceType.ANY) {
                // the specific value source type is undefined, but for scripts,
                // we need to have a specific value source
                // type to know how to handle the script values, so we fallback
                // on Bytes
                valuesSourceType = resolveScriptAny.apply(script);
            }
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<>(valuesSourceType);
            config.missing(missing);
            config.timezone(timeZone);
            config.format(resolveFormat(format, valueType, timeZone));
            config.script(createScript(script, context));
            config.scriptValueType(valueType);
            return config;
        }

        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            ValuesSourceType valuesSourceType = valueType != null ? valueType.getValuesSourceType() : CoreValuesSourceType.ANY;
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<>(valuesSourceType);
            config.missing(missing);
            config.timezone(timeZone);
            config.format(resolveFormat(format, valueType, timeZone));
            config.unmapped(true);
            if (valueType != null) {
                // todo do we really need this for unmapped?
                config.scriptValueType(valueType);
            }
            return config;
        }

        IndexFieldData<?> indexFieldData = context.getForField(fieldType);

        ValuesSourceConfig<VS> config;
        if (indexFieldData instanceof IndexNumericFieldData) {
            config = new ValuesSourceConfig<>(CoreValuesSourceType.NUMERIC);
        } else if (indexFieldData instanceof IndexGeoPointFieldData) {
            config = new ValuesSourceConfig<>(CoreValuesSourceType.GEOPOINT);
        } else if (fieldType instanceof RangeFieldMapper.RangeFieldType) {
            config = new ValuesSourceConfig<>(CoreValuesSourceType.RANGE);
        } else if (indexFieldData instanceof IndexHistogramFieldData) {
            config = new ValuesSourceConfig<>(CoreValuesSourceType.HISTOGRAM);
        } else {
            if (valueType == null) {
                config = new ValuesSourceConfig<>(CoreValuesSourceType.BYTES);
            } else {
                config = new ValuesSourceConfig<>(valueType.getValuesSourceType());
            }
        }


        config.fieldContext(new FieldContext(field, indexFieldData, fieldType));
        config.missing(missing);
        config.timezone(timeZone);
        config.script(createScript(script, context));
        config.format(fieldType.docValueFormat(format, timeZone));
        return config;
    }

    private static AggregationScript.LeafFactory createScript(Script script, QueryShardContext context) {
        if (script == null) {
            return null;
        } else {
            AggregationScript.Factory factory = context.compile(script, AggregationScript.CONTEXT);
            return factory.newFactory(script.getParams(), context.lookup());
        }
    }

    private static DocValueFormat resolveFormat(@Nullable String format, @Nullable ValueType valueType, @Nullable ZoneId tz) {
        if (valueType == null) {
            return DocValueFormat.RAW; // we can't figure it out
        }
        DocValueFormat valueFormat = valueType.defaultFormat;
        if (valueFormat instanceof DocValueFormat.Decimal && format != null) {
            valueFormat = new DocValueFormat.Decimal(format);
        }
        if (valueFormat instanceof DocValueFormat.DateTime && format != null) {
            valueFormat = new DocValueFormat.DateTime(DateFormatter.forPattern(format), tz != null ? tz : ZoneOffset.UTC,
                DateFieldMapper.Resolution.MILLISECONDS);
        }
        return valueFormat;
    }

    private final ValuesSourceType valueSourceType;
    private FieldContext fieldContext;
    private AggregationScript.LeafFactory script;
    private ValueType scriptValueType;
    private boolean unmapped = false;
    private DocValueFormat format = DocValueFormat.RAW;
    private Object missing;
    private ZoneId timeZone;

    public ValuesSourceConfig(ValuesSourceType valueSourceType) {
        this.valueSourceType = valueSourceType;
    }

    public ValuesSourceType valueSourceType() {
        return valueSourceType;
    }

    public FieldContext fieldContext() {
        return fieldContext;
    }

    public AggregationScript.LeafFactory script() {
        return script;
    }

    public boolean unmapped() {
        return unmapped;
    }

    public boolean valid() {
        return fieldContext != null || script != null || unmapped;
    }

    public ValuesSourceConfig<VS> fieldContext(FieldContext fieldContext) {
        this.fieldContext = fieldContext;
        return this;
    }

    public ValuesSourceConfig<VS> script(AggregationScript.LeafFactory script) {
        this.script = script;
        return this;
    }

    public ValuesSourceConfig<VS> scriptValueType(ValueType scriptValueType) {
        this.scriptValueType = scriptValueType;
        return this;
    }

    public ValueType scriptValueType() {
        return this.scriptValueType;
    }

    public ValuesSourceConfig<VS> unmapped(boolean unmapped) {
        this.unmapped = unmapped;
        return this;
    }

    public ValuesSourceConfig<VS> format(final DocValueFormat format) {
        this.format = format;
        return this;
    }

    public ValuesSourceConfig<VS> missing(final Object missing) {
        this.missing = missing;
        return this;
    }

    public Object missing() {
        return this.missing;
    }

    public ValuesSourceConfig<VS> timezone(final ZoneId timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public ZoneId timezone() {
        return this.timeZone;
    }

    public DocValueFormat format() {
        return format;
    }

    @Nullable
    public VS toValuesSource(QueryShardContext context) {
        return toValuesSource(context, value -> ValuesSource.Bytes.WithOrdinals.EMPTY);
    }

    /** Get a value source given its configuration. A return value of null indicates that
     *  no value source could be built. */
    @Nullable
    public VS toValuesSource(QueryShardContext context, Function<Object, ValuesSource> resolveMissingAny) {
        if (!valid()) {
            throw new IllegalStateException(
                "value source config is invalid; must have either a field context or a script or marked as unwrapped");
        }

        final VS vs;
        if (unmapped()) {
            if (missing() == null) {
                // otherwise we will have values because of the missing value
                vs = null;
            } else if (valueSourceType() == CoreValuesSourceType.ANY) {
                // TODO: Clean up special cases around CoreValuesSourceType.ANY
                vs = (VS) resolveMissingAny.apply(missing());
            } else {
                vs = (VS) valueSourceType().getEmpty();
            }
        } else {
            if (fieldContext() == null) {
                vs = (VS) valueSourceType().getScript(script(), scriptValueType());
            } else {
                if (valueSourceType() == CoreValuesSourceType.ANY) {
                    // TODO: Clean up special cases around CoreValuesSourceType.ANY
                    // falling back to bytes values
                    vs = (VS) CoreValuesSourceType.BYTES.getField(fieldContext(), script());
                } else {
                    // TODO: Better docs for Scripts vs Scripted Fields
                    vs = (VS) valueSourceType().getField(fieldContext(), script());
                }
            }
        }

        if (missing() == null) {
            return vs;
        }
        return (VS) valueSourceType().replaceMissing(vs, missing, format, context::nowInMillis);
    }
}
