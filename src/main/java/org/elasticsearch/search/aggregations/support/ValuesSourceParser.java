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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.script.*;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ValuesSourceParser<VS extends ValuesSource> {

    public static Builder any(String aggName, InternalAggregation.Type aggType, SearchContext context) {
        return new Builder<>(aggName, aggType, context, ValuesSource.class);
    }

    public static Builder<ValuesSource.Numeric> numeric(String aggName, InternalAggregation.Type aggType, SearchContext context) {
        return new Builder<>(aggName, aggType, context, ValuesSource.Numeric.class).targetValueType(ValueType.NUMERIC);
    }

    public static Builder<ValuesSource.Bytes> bytes(String aggName, InternalAggregation.Type aggType, SearchContext context) {
        return new Builder<>(aggName, aggType, context, ValuesSource.Bytes.class).targetValueType(ValueType.STRING);
    }

    public static Builder<ValuesSource.GeoPoint> geoPoint(String aggName, InternalAggregation.Type aggType, SearchContext context) {
        return new Builder<>(aggName, aggType, context, ValuesSource.GeoPoint.class).targetValueType(ValueType.GEOPOINT).scriptable(false);
    }

    private static class Input {
        String field = null;
        String script = null;
        ScriptService.ScriptType scriptType = null;
        String lang = null;
        Map<String, Object> params = null;
        ValueType valueType = null;
        String format = null;
    }

    private final String aggName;
    private final InternalAggregation.Type aggType;
    private final SearchContext context;
    private final Class<VS> valuesSourceType;

    private boolean scriptable = true;
    private boolean formattable = false;
    private ValueType targetValueType = null;
    private ScriptParameterParser scriptParameterParser = new ScriptParameterParser();

    private Input input = new Input();

    private ValuesSourceParser(String aggName, InternalAggregation.Type aggType, SearchContext context, Class<VS> valuesSourceType) {
        this.aggName = aggName;
        this.aggType = aggType;
        this.context = context;
        this.valuesSourceType = valuesSourceType;
    }

    public boolean token(String currentFieldName, XContentParser.Token token, XContentParser parser) throws IOException {
        if (token == XContentParser.Token.VALUE_STRING) {
            if ("field".equals(currentFieldName)) {
                input.field = parser.text();
            } else if (formattable && "format".equals(currentFieldName)) {
                input.format = parser.text();
            } else if (scriptable) {
                if ("value_type".equals(currentFieldName) || "valueType".equals(currentFieldName)) {
                    input.valueType = ValueType.resolveForScript(parser.text());
                    if (targetValueType != null && input.valueType.isNotA(targetValueType)) {
                        throw new SearchParseException(context, aggType.name() + " aggregation [" + aggName +
                                "] was configured with an incompatible value type [" + input.valueType + "]. [" + aggType +
                                "] aggregation can only work on value of type [" + targetValueType + "]");
                    }
                } else if (!scriptParameterParser.token(currentFieldName, token, parser)) {
                    return false;
                }
                return true;
            } else {
                return false;
            }
            return true;
        }
        if (scriptable && token == XContentParser.Token.START_OBJECT) {
            if ("params".equals(currentFieldName)) {
                input.params = parser.map();
                return true;
            }
            return false;
        }

        return false;
    }

    public ValuesSourceConfig<VS> config() {
        
        ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
        if (scriptValue != null) {
            input.script = scriptValue.script();
            input.scriptType = scriptValue.scriptType();
        }
        input.lang = scriptParameterParser.lang();
        
        ValueType valueType = input.valueType != null ? input.valueType : targetValueType;

        if (input.field == null) {
            if (input.script == null) {
                return new ValuesSourceConfig(ValuesSource.class);
            }
            Class valuesSourceType = valueType != null ? (Class<VS>) valueType.getValuesSourceType() : this.valuesSourceType;
            if (valuesSourceType == null || valuesSourceType == ValuesSource.class) {
                // the specific value source type is undefined, but for scripts, we need to have a specific value source
                // type to know how to handle the script values, so we fallback on Bytes
                valuesSourceType = ValuesSource.Bytes.class;
            }
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<VS>(valuesSourceType);
            config.format = resolveFormat(input.format, valueType);
            config.script = createScript();
            config.scriptValueType = valueType;
            return config;
        }

        FieldMapper<?> mapper = context.smartNameFieldMapperFromAnyType(input.field);
        if (mapper == null) {
            Class<VS> valuesSourceType = valueType != null ? (Class<VS>) valueType.getValuesSourceType() : this.valuesSourceType;
            ValuesSourceConfig<VS> config = new ValuesSourceConfig<>(valuesSourceType);
            config.format = resolveFormat(input.format, valueType);
            config.unmapped = true;
            if (valueType != null) {
                // todo do we really need this for unmapped?
                config.scriptValueType = valueType;
            }
            return config;
        }

        IndexFieldData<?> indexFieldData = context.fieldData().getForField(mapper);

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

        config.fieldContext = new FieldContext(input.field, indexFieldData, mapper);
        config.script = createScript();
        config.format = resolveFormat(input.format, mapper);
        return config;
    }

    private SearchScript createScript() {
        return input.script == null ? null : context.scriptService().search(context.lookup(), new Script(input.lang, input.script, input.scriptType, input.params), ScriptContext.Standard.AGGS);
    }

    private static ValueFormat resolveFormat(@Nullable String format, @Nullable ValueType valueType) {
        if (valueType == null) {
            return null; // we can't figure it out
        }
        ValueFormat valueFormat = valueType.defaultFormat;
        if (valueFormat != null && valueFormat instanceof ValueFormat.Patternable && format != null) {
            return ((ValueFormat.Patternable) valueFormat).create(format);
        }
        return valueFormat;
    }

    private static ValueFormat resolveFormat(@Nullable String format, FieldMapper mapper) {
        if (mapper instanceof  DateFieldMapper) {
            return format != null ? ValueFormat.DateTime.format(format) : ValueFormat.DateTime.mapper((DateFieldMapper) mapper);
        }
        if (mapper instanceof IpFieldMapper) {
            return ValueFormat.IPv4;
        }
        if (mapper instanceof BooleanFieldMapper) {
            return ValueFormat.BOOLEAN;
        }
        if (mapper instanceof NumberFieldMapper) {
            return format != null ? ValueFormat.Number.format(format) : ValueFormat.RAW;
        }
        return null;
    }

    public static class Builder<VS extends ValuesSource> {

        private final ValuesSourceParser<VS> parser;

        private Builder(String aggName, InternalAggregation.Type aggType, SearchContext context, Class<VS> valuesSourceType) {
            parser = new ValuesSourceParser<>(aggName, aggType, context, valuesSourceType);
        }

        public Builder<VS> scriptable(boolean scriptable) {
            parser.scriptable = scriptable;
            return this;
        }

        public Builder<VS> formattable(boolean formattable) {
            parser.formattable = formattable;
            return this;
        }

        public Builder<VS> targetValueType(ValueType valueType) {
            parser.targetValueType = valueType;
            return this;
        }

        public ValuesSourceParser<VS> build() {
            return parser;
        }
    }
}
