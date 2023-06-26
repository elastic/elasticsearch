/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneId;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;

/**
 * A configuration that tells aggregations how to retrieve data from the index
 * in order to run a specific aggregation.
 */
public class ValuesSourceConfig {

    /**
     * Given the query context and other information, decide on the input {@link ValuesSource} for this aggregation run, and construct a new
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
    public static ValuesSourceConfig resolve(
        AggregationContext context,
        ValueType userValueTypeHint,
        String field,
        Script script,
        Object missing,
        ZoneId timeZone,
        String format,
        ValuesSourceType defaultValueSourceType
    ) {

        return internalResolve(
            context,
            userValueTypeHint,
            field,
            script,
            missing,
            timeZone,
            format,
            defaultValueSourceType,
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
    public static ValuesSourceConfig resolveUnregistered(
        AggregationContext context,
        ValueType userValueTypeHint,
        String field,
        Script script,
        Object missing,
        ZoneId timeZone,
        String format,
        ValuesSourceType defaultValueSourceType
    ) {
        return internalResolve(
            context,
            userValueTypeHint,
            field,
            script,
            missing,
            timeZone,
            format,
            defaultValueSourceType,
            ValuesSourceConfig::getLegacyMapping
        );
    }

    private static ValuesSourceConfig internalResolve(
        AggregationContext context,
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
                throw new IllegalStateException("value source config is invalid; must have either a field or a script");
            }
        } else {
            // Field case
            fieldContext = context.buildFieldContext(field);
            if (fieldContext == null) {
                /* Unmapped Field Case
                 * We got here because the user specified a field, but it doesn't exist on this index, possibly because of a wildcard index
                 * pattern.  In this case, we're going to end up using the EMPTY variant of the ValuesSource, and possibly applying a user
                 * specified missing value.
                 */
                unmapped = true;
                aggregationScript = null;  // Value scripts are not allowed on unmapped fields. What would that do, anyway?
            } else {
                if (valuesSourceType == null) {
                    // We have a field, and the user didn't specify a type, so get the type from the field
                    valuesSourceType = fieldResolver.getValuesSourceType(fieldContext, userValueTypeHint, defaultValueSourceType);
                } else if (valuesSourceType != fieldResolver.getValuesSourceType(fieldContext, userValueTypeHint, defaultValueSourceType)
                    && script == null) {
                        /*
                         * This is the case where the user has specified the type they expect, but we found a field of a different type.
                         * Usually this happens because of a mapping error, e.g. an older index mapped an IP address as a keyword.  If
                         * the aggregation proceeds, it will usually break during reduction and return no results.  So instead, we fail the
                         * shard with the conflict at this point, allowing the correctly mapped shards to return results with a partial
                         * failure.
                         *
                         * Note that if a script is specified, the assumption is that the script adapts the field into the specified type,
                         * and we allow the aggregation to continue.
                         */
                        throw new IllegalArgumentException(
                            "Field type ["
                                + fieldContext.getTypeName()
                                + "] is incompatible with specified value_type ["
                                + userValueTypeHint
                                + "]"
                        );
                    }
            }
        }
        if (valuesSourceType == null) {
            valuesSourceType = defaultValueSourceType;
        }
        DocValueFormat docValueFormat = resolveFormat(format, valuesSourceType, timeZone, fieldContext);
        config = new ValuesSourceConfig(
            valuesSourceType,
            fieldContext,
            unmapped,
            aggregationScript,
            scriptValueType,
            missing,
            timeZone,
            docValueFormat,
            context
        );
        return config;
    }

    @FunctionalInterface
    private interface FieldResolver {
        ValuesSourceType getValuesSourceType(
            FieldContext fieldContext,
            ValueType userValueTypeHint,
            ValuesSourceType defaultValuesSourceType
        );

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

    private static AggregationScript.LeafFactory createScript(Script script, AggregationContext context) {
        if (script == null) {
            return null;
        } else {
            AggregationScript.Factory factory = context.compile(script, AggregationScript.CONTEXT);
            return factory.newFactory(script.getParams(), context.lookup());
        }
    }

    private static DocValueFormat resolveFormat(
        @Nullable String format,
        @Nullable ValuesSourceType valuesSourceType,
        @Nullable ZoneId tz,
        @Nullable FieldContext fieldContext
    ) {
        if (fieldContext != null) {
            return fieldContext.fieldType().docValueFormat(format, tz);
        }
        // Script or Unmapped case
        return valuesSourceType.getFormatter(format, tz);
    }

    /**
     * Special case factory method, intended to be used by aggregations which have some specialized logic for figuring out what field they
     * are operating on, for example Parent and Child join aggregations, which use the join relation to find the field they are reading from
     * rather than a user specified field.
     */
    public static ValuesSourceConfig resolveFieldOnly(MappedFieldType fieldType, AggregationContext context) {
        FieldContext fieldContext = context.buildFieldContext(fieldType);
        ValuesSourceType vstype = fieldContext.indexFieldData().getValuesSourceType();
        return new ValuesSourceConfig(vstype, fieldContext, false, null, null, null, null, null, context);
    }

    /**
     * Convenience method for creating unmapped configs
     */
    public static ValuesSourceConfig resolveUnmapped(ValuesSourceType valuesSourceType, AggregationContext context) {
        return new ValuesSourceConfig(valuesSourceType, null, true, null, null, null, null, null, context);
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
        AggregationContext context
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

        if (valid() == false) {
            // TODO: resolve no longer generates invalid configs. Once VSConfig is immutable, we can drop this check
            throw new IllegalStateException(
                "value source config is invalid; must have either a field context or a script or marked as unwrapped"
            );
        }
        valuesSource = constructValuesSource(missing, format, context);
    }

    private ValuesSource constructValuesSource(Object missing, DocValueFormat format, AggregationContext context) {
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
            return valueSourceType().replaceMissing(vs, missing, format, context);
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
     * Returns a function from the mapper that adjusts a double value to the value it would have been had it been parsed by that mapper
     * and then cast up to a double.  Used to correct precision errors.
     */
    public DoubleUnaryOperator reduceToStoredPrecisionFunction() {
        if (fieldContext() != null && fieldType() instanceof NumberFieldMapper.NumberFieldType) {
            return ((NumberFieldMapper.NumberFieldType) fieldType())::reduceToStoredPrecision;
        }
        return (value) -> value;
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

    /**
     * Build a function prepares rounding values to be called many times.
     * <p>
     * This returns a {@linkplain Function} because auto date histogram will
     * need to call it many times over the course of running the aggregation.
     */
    public Function<Rounding, Rounding.Prepared> roundingPreparer(AggregationContext context) throws IOException {
        return valuesSource.roundingPreparer(context);
    }

    /**
     * Check if this values source supports segment ordinals. Global ordinals might or might not be supported.
     * <p>
     * If this returns {@code true} then it is safe to cast it to {@link ValuesSource.Bytes.WithOrdinals}.
     * Call {@link ValuesSource.Bytes.WithOrdinals#supportsGlobalOrdinalsMapping} to find out if global ordinals are supported.
     *
     */
    public boolean hasOrdinals() {
        return valuesSource.hasOrdinals();
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
        return alignesWithSearchIndex() ? fieldType().pointReaderIfPossible() : null;
    }

    /**
     * Do {@link ValuesSource}s built by this config line up with the search
     * index of the underlying field? This'll only return true if the fields
     * is searchable and there aren't missing values or a script to confuse
     * the ordering.
     */
    public boolean alignesWithSearchIndex() {
        return script() == null && missing() == null && fieldType() != null && fieldType().isIndexed();
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
