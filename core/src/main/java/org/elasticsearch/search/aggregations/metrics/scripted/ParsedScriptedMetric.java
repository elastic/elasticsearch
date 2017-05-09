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

package org.elasticsearch.search.aggregations.metrics.scripted;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ParsedScriptedMetric extends ParsedAggregation implements ScriptedMetric {
    private List<Object> aggregation;

    @Override
    public String getType() {
        return ScriptedMetricAggregationBuilder.NAME;
    }

    @Override
    public Object aggregation() {
        if (aggregation.size() != 1) {
            throw new IllegalStateException("aggregation was not reduced");
        }
        return aggregation.get(0);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CommonFields.VALUE.getPreferredName(), aggregation());
    }

    private static final ObjectParser<ParsedScriptedMetric, Void> PARSER = new ObjectParser<>(ParsedScriptedMetric.class.getSimpleName(), true,
            ParsedScriptedMetric::new);

    static {
        declareAggregationFields(PARSER);
        PARSER.declareField((agg, value) -> agg.aggregation = Collections.singletonList(value),
                ParsedScriptedMetric::parseValue, CommonFields.VALUE, ValueType.VALUE_OBJECT_ARRAY);
    }

    private static Object parseValue(XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (token.isValue()) {
            return XContentParserUtils.parseStoredFieldsValue(parser);
        } else if (token == XContentParser.Token.START_OBJECT) {
            return parser.map();
        } else if (token == XContentParser.Token.START_ARRAY) {
            return parser.list();
        }
        return null;
    }

    public static ParsedScriptedMetric fromXContent(XContentParser parser, final String name) {
        ParsedScriptedMetric aggregation = PARSER.apply(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
