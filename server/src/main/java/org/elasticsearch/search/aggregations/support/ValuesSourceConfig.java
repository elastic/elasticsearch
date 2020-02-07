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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;

import java.time.ZoneId;

/**
 * A configuration that tells aggregations how to retrieve data from the index
 * in order to run a specific aggregation.
 */
public class ValuesSourceConfig {

    /**
     * Resolve a {@link ValuesSourceConfig} given configuration parameters.
     */
    public static  ValuesSourceConfig resolve(
        QueryShardContext context,
        ValueType valueType,
        String field, Script script,
        Object missing,
        ZoneId timeZone,
        String format, String aggregationName) {
        return resolve(context, valueType, field, script, missing, timeZone, format, CoreValuesSourceType.BYTES, aggregationName);
    }

    /**
     * Given the query context and other information, decide on the input {@link ValuesSource} for this aggretation run, and construct a new
     * {@link ValuesSourceConfig} based on that {@link ValuesSourceType}
     *
     * @param context - the query context
     * @param userValueTypeHint - User specified value type; used for missing values and scripts
     * @param field - The field being aggregated over.  At least one of field and script must not be null
     * @param script - The script the user specified.  At least one of field and script must not be null
     * @param missing - A user specified value to apply when the field is missing.  Should be of type userValueTypeHint
     * @param timeZone - Used to generate a format for dates
     * @param format - The format string to apply to this field.  Confusingly, this is used for input parsing as well as output formatting
     *               See https://github.com/elastic/elasticsearch/issues/47469
     * @param defaultValueSourceType - per-aggregation {@link ValuesSource} of last resort.
     * @param aggregationName - Name of the aggregation, generally from the aggregation builder.  This is used as a lookup key in the
     *                          {@link ValuesSourceRegistry}
     * @return - An initialized {@link ValuesSourceConfig} that will yield the appropriate {@link ValuesSourceType}
     */
    public static  ValuesSourceConfig resolve(
        QueryShardContext context,
        ValueType userValueTypeHint,
        String field, Script script,
        Object missing,
        ZoneId timeZone,
        String format,
        ValuesSourceType defaultValueSourceType,
        String aggregationName) {
        ValuesSourceConfig config;
        MappedFieldType fieldType = null;
        ValuesSourceType valuesSourceType;
        if (field == null) {
            // Stand Alone Script Case
            if (script == null) {
                throw new IllegalStateException(
                    "value source config is invalid; must have either a field context or a script or marked as unwrapped");
            }
            /*
             * This is the Stand Alone Script path.  We should have a script that will produce a value independent of the presence or
             * absence of any one field.  The type of the script is given by the userValueTypeHint field, if the user specified a type,
             * or the aggregation's default type if the user didn't.
             */
            if (userValueTypeHint != null) {
                valuesSourceType = userValueTypeHint.getValuesSourceType();
            } else {
                valuesSourceType = defaultValueSourceType;
            }
            config = new ValuesSourceConfig(valuesSourceType);
            config.script(createScript(script, context));
            config.scriptValueType(userValueTypeHint);
        } else {
            // Field case
            fieldType = context.fieldMapper(field);
            if (fieldType == null) {
                /* Unmapped Field Case
                 * We got here because the user specified a field, but it doesn't exist on this index, possibly because of a wildcard index
                 * pattern.  In this case, we're going to end up using the EMPTY variant of the ValuesSource, and possibly applying a user
                 * specified missing value.
                 */
                // TODO: This should be pluggable too; Effectively that will replace the missingAny() case from toValuesSource()
                if (userValueTypeHint != null) {
                    valuesSourceType = userValueTypeHint.getValuesSourceType();
                } else {
                    valuesSourceType = defaultValueSourceType;
                }
               config = new ValuesSourceConfig(valuesSourceType);
                // TODO: PLAN - get rid of the unmapped flag field; it's only used by valid(), and we're intending to get rid of that.
                // TODO:        Once we no longer care about unmapped, we can merge this case with  the mapped case.
                config.unmapped(true);
                if (userValueTypeHint != null) {
                    // todo do we really need this for unmapped?
                    config.scriptValueType(userValueTypeHint);
                }
            } else {
                IndexFieldData<?> indexFieldData = context.getForField(fieldType);
                valuesSourceType = context.getValuesSourceRegistry().getValuesSourceType(fieldType, aggregationName, indexFieldData,
                    userValueTypeHint, script, defaultValueSourceType);
                config = new ValuesSourceConfig(valuesSourceType);

                config.fieldContext(new FieldContext(field, indexFieldData, fieldType));
                config.script(createScript(script, context));
            }
        }
        config.format(resolveFormat(format, valuesSourceType, timeZone, fieldType));
        config.missing(missing);
        config.timezone(timeZone);
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

    private static DocValueFormat resolveFormat(@Nullable String format, @Nullable ValuesSourceType valuesSourceType, @Nullable ZoneId tz,
                                                MappedFieldType fieldType) {
        if (fieldType != null) {
            return fieldType.docValueFormat(format, tz);
        }
        // Script or Unmapped case
        return valuesSourceType.getFormatter(format, tz);
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

    public ValuesSourceConfig fieldContext(FieldContext fieldContext) {
        this.fieldContext = fieldContext;
        return this;
    }

    public ValuesSourceConfig script(AggregationScript.LeafFactory script) {
        this.script = script;
        return this;
    }

    private ValuesSourceConfig scriptValueType(ValueType scriptValueType) {
        this.scriptValueType = scriptValueType;
        return this;
    }

    public ValueType scriptValueType() {
        return this.scriptValueType;
    }

    public ValuesSourceConfig unmapped(boolean unmapped) {
        this.unmapped = unmapped;
        return this;
    }

    public ValuesSourceConfig format(final DocValueFormat format) {
        this.format = format;
        return this;
    }

    public ValuesSourceConfig missing(final Object missing) {
        this.missing = missing;
        return this;
    }

    public Object missing() {
        return this.missing;
    }

    public ValuesSourceConfig timezone(final ZoneId timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public ZoneId timezone() {
        return this.timeZone;
    }

    public DocValueFormat format() {
        return format;
    }

    /**
     * Transform the {@link ValuesSourceType} we selected in resolve into the specific {@link ValuesSource} instance to use for this shard
     * @param context - Literally just used to get the current time
     * @return - A {@link ValuesSource} ready to be read from by an aggregator
     */
    @Nullable
    // TODO: Replace QueryShardContext with a LongProvider
    public ValuesSource toValuesSource(QueryShardContext context) {
        if (!valid()) {
            // TODO: resolve no longer generates invalid configs.  Once VSConfig is immutable, we can drop this check
            throw new IllegalStateException(
                "value source config is invalid; must have either a field context or a script or marked as unwrapped");
        }

        final ValuesSource vs;
        if (unmapped()) {
            if (missing() == null) {
                // otherwise we will have values because of the missing value
                vs = null;
            } else {
                vs = valueSourceType().getEmpty();
            }
        } else {
            if (fieldContext() == null) {
                // Script case
                vs = valueSourceType().getScript(script(), scriptValueType());
            } else {
                // Field or Value Script case
                vs = valueSourceType().getField(fieldContext(), script());
            }
        }

        if (missing() == null) {
            return vs;
        }
        return valueSourceType().replaceMissing(vs, missing, format, context::nowInMillis);
    }
}
