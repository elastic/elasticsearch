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
import org.elasticsearch.search.aggregations.transformer.derivative.Derivative.GapPolicy;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Parses the histogram request
 */
public class DerivativeParser implements Aggregator.Parser {

    static final ParseField KEYED = new ParseField("keyed");
    static final ParseField FORMAT = new ParseField("format");
    static final ParseField GAP_POLICY = new ParseField("gap_policy");

    @Override
    public String type() {
        return InternalDerivative.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        GapPolicy gapPolicy = GapPolicy.ignore;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (GAP_POLICY.match(currentFieldName)) {
                    gapPolicy = GapPolicy.valueOf(parser.text());
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in aggregation [" + aggregationName + "]: ["
                            + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in aggregation [" + aggregationName + "].");
            }
        }

        return new DerivativeTransformer.Factory(aggregationName, gapPolicy);

    }
}
