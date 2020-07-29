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
import java.util.function.Function;
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
     * @return - An initialized {@link ValuesSourceConfig} that will yield the appropriate {@link ValuesSourceType}
     */
    public static ValuesSourceConfig resolve(QueryShardContext context,
                                             ValueType userValueTypeHint,
                                             String field,
                                             Script script,
                                             Object missing,
                                             ZoneId timeZone,
                                             String format,
                                             ValuesSourceType defaultValueSourceType) {

        return internalResolve(context, userValueTypeHint, field, script, missing, timeZone, format, defaultValueSourceType,
            ValuesSourceConfig::getMappingFromRegistry
        );
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
        return internalResolve(
            context,
            userValueTypeHint,
            field,
            script,
            missing,
            timeZone,
            format,
            defaultValueSourceType,
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
                                                     FieldResolver fieldResolver
                                                     ) {
        ValuesSourceConfig config;
        MappedFieldType fieldType = null;
        ValuesSourceType valuesSourceType = null;
        ValueType scriptValueType = userValueTypeHint;
        FieldContext fieldContext = null;
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
            } else {
                fieldContext = new FieldContext(fieldType.name(), context.getForField(fieldType), fieldType);
                if (valuesSourceType == null) {
                    // We have a field, and the user didn't specify a type, so get the type from the field
                    valuesSourceType = fieldResolver.getValuesSourceType(fieldContext, userValueTypeHint, defaultValueSourceType);
                }
            }
        }
        if (valuesSourceType == null) {
            valuesSourceType = defaultValueSourceType;
        }
        DocValueFormat docValueFormat = resolveFormat(format, valuesSourceType, timeZone, fieldType);
        config = new ValuesSourceConfig(
            valuesSourceType,
            fieldContext,
            unmapped,
            aggregationScript,
            scriptValueType,
            missing,
            timeZone,
            docValueFormat,
            context::nowInMillis
        );
        return config;
    }

    @FunctionalInterface
    private interface FieldResolver {
        ValuesSourceType getValuesSourceType(
            FieldContext fieldContext,
            ValueType userValueTypeHint,
            ValuesSourceType defaultValuesSourceType);

    }

    private static ValuesSourceType getMappingFromRegistry(
        FieldContext fieldContext,
        ValueType userValueTypeHint,
        ValuesSourceType defaultValuesSourceType
    ) {
        return fieldContext.indexFieldData().getValuesSourceType();
    }

    private static ValuesSourceType getLegacyMapping(
        FieldContext fieldContext,
        ValueType userValueTypeHint,
        ValuesSourceType defaultValuesSourceType
    ) {
        IndexFieldData<?> indexFieldData = fieldContext.indexFieldData();
        if (indexFieldData instanceof IndexNumericFieldData) {
            return CoreValuesSourceType.NUMERIC;
        } else if (indexFieldData instanceof IndexGeoPointFieldData) {
            return CoreValuesSourceType.GEOPOINT;
        } else if (fieldContext.fieldType() instanceof RangeFieldMapper.RangeFieldType) {
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
        FieldContext fieldContext = new FieldContext(fieldType.name(), queryShardContext.getForField(fieldType), fieldType);
        return new ValuesSourceConfig(
            fieldContext.indexFieldData().getValuesSourceType(),
            fieldContext,
            false,
            null,
            null,
            null,
            null,
            null,
            queryShardContext::nowInMillis
        );
    }

    /**
     * Convenience method for creating unmapped configs
     */
    public static ValuesSourceConfig resolveUnmapped(ValuesSourceType valuesSourceType, QueryShardContext queryShardContext) {
        return new ValuesSourceConfig(valuesSourceType, null, true, null, null, null, null, null, queryShardContext::nowInMillis);
    }

    private final ValuesSourceType valuesSourceType;
    private final FieldContext fieldContext;
    private final AggregationScript.LeafFactory script;
    private final ValueType scriptValueType;
    private final boolean unmapped;
    private final DocValueFormat format;
    private final Object missing;
    private final ZoneId timeZone;
    private final ValuesSource valuesSource;

    private ValuesSourceConfig() {
        throw new UnsupportedOperationException();
    }

    public ValuesSourceConfig(
        ValuesSourceType valuesSourceType,
        FieldContext fieldContext,
        boolean unmapped,
        AggregationScript.LeafFactory script,
        ValueType scriptValueType,
        Object missing,
        ZoneId timeZone,
        DocValueFormat format,
        LongSupplier nowSupplier
    ) {
        if (unmapped && fieldContext != null) {
            throw new IllegalStateException("value source config is invalid; marked as unmapped but specified a mapped field");
        }
        this.valuesSourceType = valuesSourceType;
        this.fieldContext = fieldContext;
        this.unmapped = unmapped;
        this.script = script;
        this.scriptValueType = scriptValueType;
        this.missing = missing;
        this.timeZone = timeZone;
        this.format = format == null ? DocValueFormat.RAW : format;

        if (!valid()) {
            // TODO: resolve no longer generates invalid configs.  Once VSConfig is immutable, we can drop this check
            throw new IllegalStateException(
                "value source config is invalid; must have either a field context or a script or marked as unwrapped");
        }
        valuesSource = ConstructValuesSource(missing, format, nowSupplier);
    }

    private ValuesSource ConstructValuesSource(Object missing, DocValueFormat format, LongSupplier nowSupplier) {
        final ValuesSource vs;
        if (this.unmapped) {
            vs = valueSourceType().getEmpty();
        } else {
            if (fieldContext() == null) {
                // Script case
                vs = valueSourceType().getScript(script(), scriptValueType());
            } else {
                // Field or Value Script case
                vs = valueSourceType().getField(fieldContext(), script());
            }
        }

        if (missing() != null) {
            return valueSourceType().replaceMissing(vs, missing, format, nowSupplier);
        } else {
            return vs;
        }
    }

    public ValuesSourceType valueSourceType() {
        return valuesSourceType;
    }

    public FieldContext fieldContext() {
        return fieldContext;
    }

    /**
     * Convenience method for looking up the mapped field type backing this values source, if it exists.
     */
    @Nullable
    public MappedFieldType fieldType() {
        return fieldContext == null ? null : fieldContext.fieldType();
    }

    public AggregationScript.LeafFactory script() {
        return script;
    }

    /**
     * Returns true if the values source configured by this object can yield values.  We might not be able to yield values if, for example,
     * the specified field does not exist on this index.
     */
    public boolean hasValues() {
        return fieldContext != null || script != null || missing != null;
    }

    public boolean valid() {
        return fieldContext != null || script != null || unmapped;
    }

    public ValueType scriptValueType() {
        return this.scriptValueType;
    }

    public Object missing() {
        return this.missing;
    }

    public ZoneId timezone() {
        return this.timeZone;
    }

    public DocValueFormat format() {
        return format;
    }

    public ValuesSource getValuesSource() {
        return valuesSource;
    }

    public boolean hasGlobalOrdinals() {
        return valuesSource.hasGlobalOrdinals();
    }

    /**
     * This method is used when an aggregation can optimize by using the indexed data instead of the doc values.  We check to see if the
     * indexed data will match the values source output (meaning there isn't a script or a missing value, since both could modify the
     * value at read time).  If the settings allow for it, we then ask the {@link ValuesSourceType} to build the actual point reader
     * based on the field type.  This allows for a point of extensibility in plugins.
     *
     * @return null if we cannot apply the optimization, otherwise the point reader function.
     */
    @Nullable
    public Function<byte[], Number> getPointReaderOrNull() {
        MappedFieldType fieldType = fieldType();
        if (fieldType != null && script() == null && missing() == null) {
            return fieldType.pointReaderIfPossible();
        }
        return null;
    }

    /**
     * Returns a human readable description of this values source, for use in error messages and similar.
     */
    public String getDescription() {
        if (script != null) {
            return "Script yielding [" + (scriptValueType != null ? scriptValueType.getPreferredName() : "unknown type") + "]";
        }

        MappedFieldType fieldType = fieldType();
        if (fieldType != null) {
            return "Field [" + fieldType.name() + "] of type [" + fieldType.typeName() + "]";
        }
        return "unmapped field";
    }
}
