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
package org.elasticsearch.search.aggregations.metrics.tophits;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TopHitsParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalTopHits.TYPE.name();
    }

    @Override
    public TopHitsAggregatorBuilder parse(String aggregationName, XContentParser parser, QueryParseContext context)
            throws IOException {
        TopHitsAggregatorBuilder factory = new TopHitsAggregatorBuilder(aggregationName);
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.FROM_FIELD)) {
                    factory.from(parser.intValue());
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.SIZE_FIELD)) {
                    factory.size(parser.intValue());
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.VERSION_FIELD)) {
                    factory.version(parser.booleanValue());
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.EXPLAIN_FIELD)) {
                    factory.explain(parser.booleanValue());
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.TRACK_SCORES_FIELD)) {
                    factory.trackScores(parser.booleanValue());
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder._SOURCE_FIELD)) {
                    factory.fetchSource(FetchSourceContext.parse(parser, context));
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.FIELDS_FIELD)) {
                    List<String> fieldNames = new ArrayList<>();
                    fieldNames.add(parser.text());
                    factory.fields(fieldNames);
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.SORT_FIELD)) {
                    factory.sort(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder._SOURCE_FIELD)) {
                    factory.fetchSource(FetchSourceContext.parse(parser, context));
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.SCRIPT_FIELDS_FIELD)) {
                    List<ScriptField> scriptFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        String scriptFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token == XContentParser.Token.START_OBJECT) {
                            Script script = null;
                            boolean ignoreFailure = false;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.SCRIPT_FIELD)) {
                                        script = Script.parse(parser, context.parseFieldMatcher());
                                    } else if (context.parseFieldMatcher().match(currentFieldName,
                                            SearchSourceBuilder.IGNORE_FAILURE_FIELD)) {
                                        ignoreFailure = parser.booleanValue();
                                    } else {
                                        throw new ParsingException(parser.getTokenLocation(),
                                                "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                                parser.getTokenLocation());
                                    }
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.SCRIPT_FIELD)) {
                                        script = Script.parse(parser, context.parseFieldMatcher());
                                    } else {
                                        throw new ParsingException(parser.getTokenLocation(),
                                                "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                                parser.getTokenLocation());
                                    }
                                } else {
                                    throw new ParsingException(parser.getTokenLocation(),
                                            "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
                                }
                            }
                            scriptFields.add(new ScriptField(scriptFieldName, script, ignoreFailure));
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT
                                    + "] in [" + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                    factory.scriptFields(scriptFields);
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.HIGHLIGHT_FIELD)) {
                    factory.highlighter(HighlightBuilder.fromXContent(context));
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.SORT_FIELD)) {
                    List<SortBuilder<?>> sorts = SortBuilder.fromXContent(context);
                    factory.sorts(sorts);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {

                if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.FIELDS_FIELD)) {
                    List<String> fieldNames = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            fieldNames.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING
                                    + "] in [" + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                    factory.fields(fieldNames);
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.FIELDDATA_FIELDS_FIELD)) {
                    List<String> fieldDataFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            fieldDataFields.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING
                                    + "] in [" + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                    factory.fieldDataFields(fieldDataFields);
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder.SORT_FIELD)) {
                    List<SortBuilder<?>> sorts = SortBuilder.fromXContent(context);
                    factory.sorts(sorts);
                } else if (context.parseFieldMatcher().match(currentFieldName, SearchSourceBuilder._SOURCE_FIELD)) {
                    factory.fetchSource(FetchSourceContext.parse(parser, context));
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                        parser.getTokenLocation());
            }
        }
        return factory;
    }

    @Override
    public TopHitsAggregatorBuilder getFactoryPrototypes() {
        return TopHitsAggregatorBuilder.PROTOTYPE;
    }

}
