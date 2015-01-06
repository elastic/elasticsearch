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
package org.elasticsearch.search.aggregations.transformer.derivative;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Parses the histogram request
 */
public class DerivativeParser implements Aggregator.Parser {

    static final ParseField EXTENDED_BOUNDS = new ParseField("extended_bounds");

    @Override
    public String type() {
        return InternalDerivative.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        boolean keyed = false;
        String format = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in aggregation [" + aggregationName + "]: ["
                            + currentFieldName + "].");
                }
                // NOCOMMIT implement order
                // } else if (token == XContentParser.Token.START_OBJECT) {
                // if ("order".equals(currentFieldName)) {
                // while ((token = parser.nextToken()) !=
                // XContentParser.Token.END_OBJECT) {
                // if (token == XContentParser.Token.FIELD_NAME) {
                // currentFieldName = parser.currentName();
                // } else if (token == XContentParser.Token.VALUE_STRING) {
                // String dir = parser.text();
                // boolean asc = "asc".equals(dir);
                // if (!asc && !"desc".equals(dir)) {
                // throw new SearchParseException(context,
                // "Unknown order direction [" + dir + "] in aggregation [" +
                // aggregationName + "]. Should be either [asc] or [desc]");
                // }
                // order = resolveOrder(currentFieldName, asc);
                // }
                // }
                // } else {
                // throw new SearchParseException(context, "Unknown key for a "
                // + token + " in aggregation [" + aggregationName + "]: [" +
                // currentFieldName + "].");
                // }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("format".equals(currentFieldName)) {
                    format = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in aggregation [" + aggregationName + "]: ["
                            + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in aggregation [" + aggregationName + "].");
            }
        }

        ValueFormatter formatter = null;
        if (format != null) {
            ValueType valueType = ValueType.LONG; // NOCOMMIT need to detect this somehow
            ValueFormat valueFormat = valueType.defaultFormat();
            if (valueFormat != null && valueFormat instanceof ValueFormat.Patternable && format != null) {
                formatter = ((ValueFormat.Patternable) valueFormat).create(format).formatter();
            } else {
                throw new SearchParseException(context, "Cannot resolve format [" + format + "] in aggregation [" + aggregationName + "].");
            }
        }

        return new DerivativeTransformer.Factory(aggregationName, keyed, formatter);

    }
}
