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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// NORELEASE remove this class when aggs refactoring complete
/**
 * @deprecated use {@link AbstractValuesSourceParser} instead. This class will
 *             be removed when aggs refactoring is complete.
 */
@Deprecated
public class ValuesSourceParser<VS extends ValuesSource> {

    static final ParseField TIME_ZONE = new ParseField("time_zone");

    public static Builder any(String aggName, InternalAggregation.Type aggType, SearchContext context) {
        return new Builder<>(aggName, aggType, context, ValuesSource.class, ValuesSourceType.ANY);
    }

    public static Builder<ValuesSource.Numeric> numeric(String aggName, InternalAggregation.Type aggType, SearchContext context) {
        return new Builder<>(aggName, aggType, context, ValuesSource.Numeric.class, ValuesSourceType.NUMERIC)
                .targetValueType(ValueType.NUMERIC);
    }

    public static Builder<ValuesSource.Bytes> bytes(String aggName, InternalAggregation.Type aggType, SearchContext context) {
        return new Builder<>(aggName, aggType, context, ValuesSource.Bytes.class, ValuesSourceType.BYTES).targetValueType(ValueType.STRING);
    }

    public static Builder<ValuesSource.GeoPoint> geoPoint(String aggName, InternalAggregation.Type aggType, SearchContext context) {
        return new Builder<>(aggName, aggType, context, ValuesSource.GeoPoint.class, ValuesSourceType.GEOPOINT).targetValueType(
                ValueType.GEOPOINT).scriptable(false);
    }

    // NORELEASE remove this class when aggs refactoring complete
    /**
     * @deprecated use {@link AbstractValuesSourceParser} instead. This class
     *             will be removed when aggs refactoring is complete.
     */
    @Deprecated
    public static class Input<VS> {
        String field = null;
        Script script = null;
        @Deprecated
        Map<String, Object> params = null; // TODO Remove in 3.0
        ValueType valueType = null;
        String format = null;
        Object missing = null;
        ValuesSourceType valuesSourceType = null;
        ValueType targetValueType = null;
        DateTimeZone timezone = DateTimeZone.UTC;

        public boolean valid() {
            return field != null || script != null;
        }

        public DateTimeZone timezone() {
            return this.timezone;
        }
    }

    private final String aggName;
    private final InternalAggregation.Type aggType;
    private final SearchContext context;

    private boolean scriptable = true;
    private boolean formattable = false;
    private boolean timezoneAware = false;
    private ScriptParameterParser scriptParameterParser = new ScriptParameterParser();

    private Input<VS> input = new Input<VS>();

    private ValuesSourceParser(String aggName, InternalAggregation.Type aggType, SearchContext context, ValuesSourceType valuesSourceType) {
        this.aggName = aggName;
        this.aggType = aggType;
        this.context = context;
        input.valuesSourceType = valuesSourceType;
    }

    public boolean token(String currentFieldName, XContentParser.Token token, XContentParser parser) throws IOException {
        if ("missing".equals(currentFieldName) && token.isValue()) {
            input.missing = parser.objectText();
            return true;
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            if ("field".equals(currentFieldName)) {
                input.field = parser.text();
            } else if (formattable && "format".equals(currentFieldName)) {
                input.format = parser.text();
            } else if (timezoneAware && context.parseFieldMatcher().match(currentFieldName, TIME_ZONE)) {
                input.timezone = DateTimeZone.forID(parser.text());
            } else if (scriptable) {
                if ("value_type".equals(currentFieldName) || "valueType".equals(currentFieldName)) {
                    input.valueType = ValueType.resolveForScript(parser.text());
                    if (input.targetValueType != null && input.valueType.isNotA(input.targetValueType)) {
                        throw new SearchParseException(context, aggType.name() + " aggregation [" + aggName
                                + "] was configured with an incompatible value type [" + input.valueType + "]. [" + aggType
                                + "] aggregation can only work on value of type [" + input.targetValueType + "]", parser.getTokenLocation());
                    }
                } else if (!scriptParameterParser.token(currentFieldName, token, parser, context.parseFieldMatcher())) {
                    return false;
                }
                return true;
            } else {
                return false;
            }
            return true;
        }
        if (token == XContentParser.Token.VALUE_NUMBER) {
            if (timezoneAware && context.parseFieldMatcher().match(currentFieldName, TIME_ZONE)) {
                input.timezone = DateTimeZone.forOffsetHours(parser.intValue());
            } else {
                return false;
            }
            return true;
        }
        if (scriptable && token == XContentParser.Token.START_OBJECT) {
            if (context.parseFieldMatcher().match(currentFieldName, ScriptField.SCRIPT)) {
                input.script = Script.parse(parser, context.parseFieldMatcher());
                return true;
            } else if ("params".equals(currentFieldName)) {
                input.params = parser.map();
                return true;
            }
            return false;
        }

        return false;
    }

    public Input<VS> input() {
        if (input.script == null) { // Didn't find anything using the new API so
                                    // try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
            if (scriptValue != null) {
                if (input.params == null) {
                    input.params = new HashMap<>();
                }
                input.script = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), input.params);
            }
        }

        return input;
    }

    // NORELEASE remove this class when aggs refactoring complete
    /**
     * @deprecated use {@link AbstractValuesSourceParser} instead. This class
     *             will be removed when aggs refactoring is complete.
     */
    @Deprecated
    public static class Builder<VS extends ValuesSource> {

        private final ValuesSourceParser<VS> parser;

        private Builder(String aggName, InternalAggregation.Type aggType, SearchContext context, Class<VS> valuesSourcecClass,
                ValuesSourceType valuesSourceType) {
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

        public Builder<VS> timezoneAware(boolean timezoneAware) {
            parser.timezoneAware = timezoneAware;
            return this;
        }

        public Builder<VS> targetValueType(ValueType valueType) {
            parser.input.targetValueType = valueType;
            return this;
        }

        public ValuesSourceParser<VS> build() {
            return parser;
        }
    }
}
