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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.function.Function;

public class ValueSourceParser implements Aggregator.Parser {
    /**
     * Start building a new ValueSourceParser. It will be formattable and scriptable and not time zone aware by default but those can be
     * flipped by calling builder methods.
     */
    public static Builder builder(InternalAggregation.Type type) {
        return new Builder(new ObjectParser<>(type.name()));
    }
    public static class Builder {
        private final ObjectParser<ValuesSourceAggregatorBuilder<?, ?>, QueryParseContext> objectParser;
        private boolean formattable = true;
        private boolean scriptable = true;

        private Builder(ObjectParser<ValuesSourceAggregatorBuilder<?, ?>, QueryParseContext> objectParser) {
            this.objectParser = objectParser;
            objectParser.declareString(ValuesSourceAggregatorBuilder::field, FIELD);
            objectParser.declareField((parser, builder, context) -> builder.missing(parser.objectText()), MISSING,
                    ObjectParser.ValueType.VALUE);
            objectParser.declareString((v, s) -> {
                ValueType valueType = ValueType.resolveForScript(s);
                if (v.getTargetValueType() != null && valueType.isNotA(v.getTargetValueType())) {
                    throw new IllegalArgumentException("Expected a [" + v.getTargetValueType() + "] but got [" + valueType + "]");
                }
                v.valueType(valueType);
            }, VALUE_TYPE);
        }

        public ValueSourceParser build(Function<String, ? extends ValuesSourceAggregatorBuilder<?, ?>> builderBuilder) {
            if (formattable) {
                objectParser.declareString(ValuesSourceAggregatorBuilder::format, FORMAT);
            }
            if (scriptable) {
                objectParser.declareField((p, v, c) -> v.script(Script.parse(p, c.parseFieldMatcher())),
                        ScriptField.SCRIPT, ObjectParser.ValueType.OBJECT);
            }
            return new ValueSourceParser(builderBuilder, objectParser);
        }

        /**
         * The aggregation supports a "time_zone" field.
         */
        public Builder timeZoneAware() {
            objectParser.declareTimeZone(ValuesSourceAggregatorBuilder::timeZone, TIME_ZONE);
            return this;
        }

        /**
         * The aggregation doesn't support a "format" field.
         */
        public Builder notFormattable() {
            formattable = false;
            return this;
        }

        /**
         * The aggregation doesn't support a "script" object.
         */
        public Builder notScriptable() {
            scriptable = false;
            return this;
        }
    }

    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField MISSING = new ParseField("missing");
    private static final ParseField TIME_ZONE = new ParseField("time_zone");
    private static final ParseField FORMAT = new ParseField("format");
    private static final ParseField VALUE_TYPE = new ParseField("value_type");
    
    private final Function<String, ? extends ValuesSourceAggregatorBuilder<?, ?>> builderBuilder;
    private final ObjectParser<ValuesSourceAggregatorBuilder<?, ?>, QueryParseContext> objectParser;

    public ValueSourceParser(Function<String, ? extends ValuesSourceAggregatorBuilder<?, ?>> builderBuilder,
            ObjectParser<ValuesSourceAggregatorBuilder<?, ?>, QueryParseContext> objectParser) {
        this.builderBuilder = builderBuilder;
        this.objectParser = objectParser;
    }

    @Override
    public final ValuesSourceAggregatorBuilder<?, ?> parse(String aggregationName, XContentParser parser, QueryParseContext context)
            throws IOException {
        return objectParser.parse(parser, builderBuilder.apply(aggregationName), context);
    }
}
