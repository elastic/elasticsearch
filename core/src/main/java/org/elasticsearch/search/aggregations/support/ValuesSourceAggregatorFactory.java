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
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class ValuesSourceAggregatorFactory<VS extends ValuesSource> extends AggregatorFactory {

    public static abstract class LeafOnly<VS extends ValuesSource> extends ValuesSourceAggregatorFactory<VS> {

        protected LeafOnly(String name, String type, ValuesSourceParser.Input<VS> input) {
            super(name, type, input);
        }

        protected LeafOnly(String name, String type, Class<VS> valuesSourceType, ValueType targetValueType) {
            super(name, type, valuesSourceType, targetValueType);
        }

        @Override
        public AggregatorFactory subFactories(AggregatorFactories subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }
    }

    private final Class<VS> valuesSourceType;
    private final ValueType targetValueType;
    private String field = null;
    private Script script = null;
    private ValueType valueType = null;
    private String format = null;
    private Object missing = null;
    protected ValuesSourceConfig<VS> config;
    private DateTimeZone timeZone;

    // NORELEASE remove this method when aggs refactoring complete
    /**
     * This constructor remains here until all subclasses have been moved to the
     * new constructor. This also means moving from using
     * {@link ValuesSourceParser} to using {@link AbstractValuesSourceParser}.
     */
    @Deprecated
    protected ValuesSourceAggregatorFactory(String name, String type, ValuesSourceParser.Input<VS> input) {
        super(name, type);
        this.valuesSourceType = input.valuesSourceType;
        this.targetValueType = input.targetValueType;
        this.field = input.field;
        this.script = input.script;
        this.valueType = input.valueType;
        this.format = input.format;
        this.missing = input.missing;
        this.timeZone = input.timezone;
    }

    protected ValuesSourceAggregatorFactory(String name, String type, Class<VS> valuesSourceType, ValueType targetValueType) {
        super(name, type);
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
    }

    public void field(String field) {
        this.field = field;
    }

    public void script(Script script) {
        this.script = script;
    }

    public void valueType(ValueType valueType) {
        this.valueType = valueType;
    }

    public void format(String format) {
        this.format = format;
    }

    public void missing(Object missing) {
        this.missing = missing;
    }

    public void timeZone(DateTimeZone timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public void doInit(AggregationContext context) {
        this.config = config(context);
        if (config == null || !config.valid()) {
            resolveValuesSourceConfigFromAncestors(name, this.parent, config.valueSourceType());
        }

    }

    @Override
    public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        VS vs = context.valuesSource(config, context.searchContext());
        if (vs == null) {
            return createUnmapped(context, parent, pipelineAggregators, metaData);
        }
        return doCreateInternal(vs, context, parent, collectsFromSingleBucket, pipelineAggregators, metaData);
    }

    @Override
    public void doValidate() {
    }

    public ValuesSourceConfig<VS> config(AggregationContext context) {

        ValueType valueType = this.valueType != null ? this.valueType : targetValueType;

        if (field == null) {
            if (script == null) {
                ValuesSourceConfig<VS> config = new ValuesSourceConfig(ValuesSource.class);
                config.format = resolveFormat(null, valueType);
                return config;
            }
            Class valuesSourceType = valueType != null ? (Class<VS>) valueType.getValuesSourceType() : this.valuesSourceType;
            if (valuesSourceType == null || valuesSourceType == ValuesSource.class) {
                // the specific value source type is undefined, but for scripts,
                // we need to have a specific value source
                // type to know how to handle the script values, so we fallback
                // on Bytes
                valuesSourceType = ValuesSource.Bytes.class;
            }
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<VS>(valuesSourceType);
            config.missing = missing;
            config.format = resolveFormat(format, valueType);
            config.script = createScript(script, context.searchContext());
            config.scriptValueType = valueType;
            return config;
        }

        MappedFieldType fieldType = context.searchContext().smartNameFieldTypeFromAnyType(field);
        if (fieldType == null) {
            Class<VS> valuesSourceType = valueType != null ? (Class<VS>) valueType.getValuesSourceType() : this.valuesSourceType;
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<>(valuesSourceType);
            config.missing = missing;
            config.format = resolveFormat(format, valueType);
            config.unmapped = true;
            if (valueType != null) {
                // todo do we really need this for unmapped?
                config.scriptValueType = valueType;
            }
            return config;
        }

        IndexFieldData<?> indexFieldData = context.searchContext().fieldData().getForField(fieldType);

        ValuesSourceConfig config;
        if (valuesSourceType == ValuesSource.class) {
            if (indexFieldData instanceof IndexNumericFieldData) {
                config = new ValuesSourceConfig<>(ValuesSource.Numeric.class);
            } else if (indexFieldData instanceof IndexGeoPointFieldData) {
                config = new ValuesSourceConfig<>(ValuesSource.GeoPoint.class);
            } else {
                config = new ValuesSourceConfig<>(ValuesSource.Bytes.class);
            }
        } else {
            config = new ValuesSourceConfig(valuesSourceType);
        }

        config.fieldContext = new FieldContext(field, indexFieldData, fieldType);
        config.missing = missing;
        config.script = createScript(script, context.searchContext());
        config.format = resolveFormat(format, this.timeZone, fieldType);
        return config;
    }

    private SearchScript createScript(Script script, SearchContext context) {
        return script == null ? null : context.scriptService().search(context.lookup(), script, ScriptContext.Standard.AGGS);
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

    protected abstract Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException;

    protected abstract Aggregator doCreateInternal(VS valuesSource, AggregationContext aggregationContext, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException;

    private void resolveValuesSourceConfigFromAncestors(String aggName, AggregatorFactory parent, Class<VS> requiredValuesSourceType) {
        ValuesSourceConfig config;
        while (parent != null) {
            if (parent instanceof ValuesSourceAggregatorFactory) {
                config = ((ValuesSourceAggregatorFactory) parent).config;
                if (config != null && config.valid()) {
                    if (requiredValuesSourceType == null || requiredValuesSourceType.isAssignableFrom(config.valueSourceType)) {
                        ValueFormat format = config.format;
                        this.config = config;
                        // if the user explicitly defined a format pattern,
                        // we'll do our best to keep it even when we inherit the
                        // value source form one of the ancestor aggregations
                        if (this.config.formatPattern != null && format != null && format instanceof ValueFormat.Patternable) {
                            this.config.format = ((ValueFormat.Patternable) format).create(this.config.formatPattern);
                        }
                        return;
                    }
                }
            }
            parent = parent.parent();
        }
        throw new AggregationExecutionException("could not find the appropriate value context to perform aggregation [" + aggName + "]");
    }
}
