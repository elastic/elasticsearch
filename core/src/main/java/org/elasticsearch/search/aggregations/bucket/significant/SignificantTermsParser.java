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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseFieldRegistry;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.bucket.terms.TermsParser;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.AnyValuesSourceParser;

import java.io.IOException;

public class SignificantTermsParser extends AnyValuesSourceParser {

    private final ObjectParser<SignificantTermsAggregationBuilder, QueryParseContext> parser;

    public SignificantTermsParser(ParseFieldRegistry<SignificanceHeuristicParser> significanceHeuristicParserRegistry) {
        parser = new ObjectParser<>(SignificantTermsAggregationBuilder.NAME);
        addFields(parser, true, true);

        parser.declareInt(SignificantTermsAggregationBuilder::shardSize, TermsParser.SHARD_SIZE_FIELD_NAME);

        parser.declareLong(SignificantTermsAggregationBuilder::minDocCount, TermsParser.MIN_DOC_COUNT_FIELD_NAME);

        parser.declareLong(SignificantTermsAggregationBuilder::shardMinDocCount, TermsParser.SHARD_MIN_DOC_COUNT_FIELD_NAME);

        parser.declareInt(SignificantTermsAggregationBuilder::size, TermsParser.REQUIRED_SIZE_FIELD_NAME);

        parser.declareString(SignificantTermsAggregationBuilder::executionHint, TermsParser.EXECUTION_HINT_FIELD_NAME);

        parser.declareObject((b, v) -> { if (v.isPresent()) b.backgroundFilter(v.get()); },
                (parser, context) -> context.parseInnerQueryBuilder(),
                SignificantTermsAggregationBuilder.BACKGROUND_FILTER);

        parser.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
                IncludeExclude::parseInclude, IncludeExclude.INCLUDE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);

        parser.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
                IncludeExclude::parseExclude, IncludeExclude.EXCLUDE_FIELD, ObjectParser.ValueType.STRING_ARRAY);

        for (String name : significanceHeuristicParserRegistry.getNames()) {
            parser.declareObject(SignificantTermsAggregationBuilder::significanceHeuristic,
                    (parser, context) -> {
                        SignificanceHeuristicParser significanceHeuristicParser = significanceHeuristicParserRegistry
                                .lookupReturningNullIfNotFound(name, context.getParseFieldMatcher());
                        return significanceHeuristicParser.parse(context);
                    },
                    new ParseField(name));
        }
    }

    @Override
    public AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        return parser.parse(context.parser(), new SignificantTermsAggregationBuilder(aggregationName, null), context);
    }

}
