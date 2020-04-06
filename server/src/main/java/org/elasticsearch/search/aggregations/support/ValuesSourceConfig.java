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
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;

import java.time.ZoneId;
import java.util.function.LongSupplier;

/**
 * A configuration that tells aggregations how to retrieve data from the index
 * in order to run a specific aggregation.
 */
public class ValuesSourceConfig {

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
    public static ValuesSourceConfig resolve(QueryShardContext context,
                                             ValueType userValueTypeHint,
                                             String field,
                                             Script script,
                                             Object missing,
                                             ZoneId timeZone,
                                             String format,
                                             ValuesSourceType defaultValueSourceType,
                                             String aggregationName) {

        return internalResolve(context, userValueTypeHint, field, script, missing, timeZone, format, defaultValueSourceType,
            aggregationName, ValuesSourceConfig::getMappingFromRegistry);
    }

    /**
     * AKA legacy resolve.  This method should be called by aggregations not supported by the {@link ValuesSourceRegistry}, to use the
     * pre-registry logic to decide on the {@link ValuesSourceType}.  New aggregations which extend from
     * {@link ValuesSourceAggregationBuilder} should not use this method, preferring {@link ValuesSourceConfig#resolve} instead.
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
     * @return - An initialized {@link ValuesSourceConfig} that will yield the appropriate {@link ValuesSourceType}
     */
    public static ValuesSourceConfig resolveUnregistered(QueryShardContext context,
                                                         ValueType userValueTypeHint,
                                                         String field,
                                                         Script script,
                                                         Object missing,
                                                         ZoneId timeZone,
                                                         String format,
                                                         ValuesSourceType defaultValueSourceType) {
        return internalResolve(context, userValueTypeHint, field, script, missing, timeZone, format, defaultValueSourceType, null,
            ValuesSourceConfig::getLegacyMapping);
    }

    private static ValuesSourceConfig internalResolve(QueryShardContext context,
                                                     ValueType userValueTypeHint,
                                                     String field,
                                                     Script script,
                                                     Object missing,
                                                     ZoneId timeZone,
                                                     String format,
                                                     ValuesSourceType defaultValueSourceType,
                                                     String aggregationName,
                                                     FieldResolver fieldResolver
                                                     ) {
        ValuesSourceConfig config;
        MappedFieldType fieldType = null;
        ValuesSourceType valuesSourceType = null;
        ValueType scriptValueType = userValueTypeHint;
        AggregationScript.LeafFactory aggregationScript = createScript(script, context); // returns null if script is null
        boolean unmapped = false;
        if (userValueTypeHint != null) {
            // If the user gave us a type hint, respect that.
            valuesSourceType = userValueTypeHint.getValuesSourceType();
        }
        if (field == null) {
            if (script == null) {
                throw new IllegalStateException(
                    "value source config is invalid; must have either a field or a script");
            }
        } else {
            // Field case
            fieldType = context.fieldMapper(field);
            if (fieldType == null) {
                /* Unmapped Field Case
                 * We got here because the user specified a field, but it doesn't exist on this index, possibly because of a wildcard index
                 * pattern.  In this case, we're going to end up using the EMPTY variant of the ValuesSource, and possibly applying a user
                 * specified missing value.
                 */
                unmapped = true;
                aggregationScript = null;  // Value scripts are not allowed on unmapped fields.  What would that do, anyway?
            } else if (valuesSourceType == null) {
                // We have a field, and the user didn't specify a type, so get the type from the field
                valuesSourceType = fieldResolver.getValuesSourceType(context, fieldType, aggregationName, userValueTypeHint,
                    defaultValueSourceType);
            }
        }
        if (valuesSourceType == null) {
            valuesSourceType = defaultValueSourceType;
        }
        config = new ValuesSourceConfig(valuesSourceType, fieldType, unmapped, aggregationScript, scriptValueType , context);
        config.format(resolveFormat(format, valuesSourceType, timeZone, fieldType));
        config.missing(missing);
        config.timezone(timeZone);
        return config;
    }

    @FunctionalInterface
    private interface FieldResolver {
        ValuesSourceType getValuesSourceType(
            QueryShardContext context,
            MappedFieldType fieldType,
            String aggregationName,
            ValueType userValueTypeHint,
            ValuesSourceType defaultValuesSourceType);

    }

    private static ValuesSourceType getMappingFromRegistry(
            QueryShardContext context,
            MappedFieldType fieldType,
            String aggregationName,
            ValueType userValueTypeHint,
            ValuesSourceType defaultValuesSourceType) {
        IndexFieldData<?> indexFieldData = context.getForField(fieldType);
         return context.getValuesSourceRegistry().getValuesSourceType(fieldType, aggregationName, indexFieldData,
            userValueTypeHint, defaultValuesSourceType);
    }

    private static ValuesSourceType getLegacyMapping(
            QueryShardContext context,
            MappedFieldType fieldType,
            String aggregationName,
            ValueType userValueTypeHint,
            ValuesSourceType defaultValuesSourceType) {
        IndexFieldData<?> indexFieldData = context.getForField(fieldType);
        if (indexFieldData instanceof IndexNumericFieldData) {
            return CoreValuesSourceType.NUMERIC;
        } else if (indexFieldData instanceof IndexGeoPointFieldData) {
            return CoreValuesSourceType.GEOPOINT;
        } else if (fieldType instanceof RangeFieldMapper.RangeFieldType) {
            return CoreValuesSourceType.RANGE;
        } else {
            if (userValueTypeHint == null) {
                return defaultValuesSourceType;
            } else {
                return userValueTypeHint.getValuesSourceType();
            }
        }

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

    /**
     * Special case factory method, intended to be used by aggregations which have some specialized logic for figuring out what field they
     * are operating on, for example Parent and Child join aggregations, which use the join relation to find the field they are reading from
     * rather than a user specified field.
     */
    public static ValuesSourceConfig resolveFieldOnly(MappedFieldType fieldType,
                                                      QueryShardContext queryShardContext) {
        return new ValuesSourceConfig(fieldType.getValuesSourceType(), fieldType, false, null, null, queryShardContext);
    }

    /**
     * Convenience method for creating unmapped configs
     */
    public static ValuesSourceConfig resolveUnmapped(ValuesSourceType valuesSourceType, QueryShardContext queryShardContext) {
        return new ValuesSourceConfig(valuesSourceType, null, true, null, null, queryShardContext);
    }

    private final ValuesSourceType valuesSourceType;
    private FieldContext fieldContext;
    private AggregationScript.LeafFactory script;
    private ValueType scriptValueType;
    private boolean unmapped;
    private DocValueFormat format = DocValueFormat.RAW;
    private Object missing;
    private ZoneId timeZone;
    private LongSupplier nowSupplier;


    public ValuesSourceConfig(ValuesSourceType valuesSourceType,
                              MappedFieldType fieldType,
                              boolean unmapped,
                              AggregationScript.LeafFactory script,
                              ValueType scriptValueType,
                              QueryShardContext queryShardContext) {
        if (unmapped && fieldType != null) {
            throw new IllegalStateException("value source config is invalid; marked as unmapped but specified a mapped field");
        }
        this.valuesSourceType = valuesSourceType;
        if (fieldType != null) {
            this.fieldContext = new FieldContext(fieldType.name(), queryShardContext.getForField(fieldType), fieldType);
        }
        this.unmapped = unmapped;
        this.script = script;
        this.scriptValueType = scriptValueType;
        this.nowSupplier = queryShardContext::nowInMillis;

    }

    public ValuesSourceType valueSourceType() {
        return valuesSourceType;
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

    public ValueType scriptValueType() {
        return this.scriptValueType;
    }

    private ValuesSourceConfig format(final DocValueFormat format) {
        this.format = format;
        return this;
    }

    private ValuesSourceConfig missing(final Object missing) {
        this.missing = missing;
        return this;
    }

    public Object missing() {
        return this.missing;
    }

    private ValuesSourceConfig timezone(final ZoneId timeZone) {
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
     * @return - A {@link ValuesSource} ready to be read from by an aggregator
     */
    @Nullable
    public ValuesSource toValuesSource() {
        if (!valid()) {
            // TODO: resolve no longer generates invalid configs.  Once VSConfig is immutable, we can drop this check
            throw new IllegalStateException(
                "value source config is invalid; must have either a field context or a script or marked as unwrapped");
        }

        final ValuesSource vs;
        if (unmapped()) {
            if (missing() == null) {
                /* Null values source signals to the AggregationBuilder to use the createUnmapped method, which aggregator factories can
                 * override to provide an aggregator optimized to return empty values
                 */
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
        return valueSourceType().replaceMissing(vs, missing, format, nowSupplier);
    }
}
