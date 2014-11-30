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

package org.elasticsearch.search.reducers;

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReducerParsers {

    public static final Pattern VALID_REDUCER_NAME = Pattern.compile("[^\\[\\]>]+");
    private final ImmutableMap<String, Reducer.Parser> parsers;


    /**
     * Constructs the ReducerParsers out of all the given parsers
     *
     * @param parsers The available reducer parsers (dynamically injected by the {@link org.elasticsearch.search.reducers.ReductionModule}).
     */
    @Inject
    public ReducerParsers(Set<Reducer.Parser> parsers) {
        MapBuilder<String, Reducer.Parser> builder = MapBuilder.newMapBuilder();
        for (Reducer.Parser parser : parsers) {
            for (String type : parser.types()) {
                builder.put(type, parser);
            }
        }
        this.parsers = builder.immutableMap();
    }

    /**
     * Returns the parser that is registered under the given reduction type.
     *
     * @param type  The reduction type
     * @return      The parser associated with the given reduction type.
     */
    public Reducer.Parser parser(String type) {
        return parsers.get(type);
    }

    /**
     * Parses the reduction request recursively generating reducer factories in turn.
     *
     * @param parser    The input xcontent that will be parsed.
     * @param context   The search context.
     *
     * @return          The parsed reducer factories.
     *
     * @throws IOException When parsing fails for unknown reasons.
     */
    public ReducerFactories parseReducers(XContentParser parser, SearchContext context) throws IOException{
        return parseReducers(parser, context, 0);
    }
    
    private ReducerFactories parseReducers(XContentParser parser, SearchContext context, int level) throws IOException {
        Matcher validReducerMatcher = VALID_REDUCER_NAME.matcher("");
        ReducerFactories.Builder factories = new ReducerFactories.Builder();

        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.START_OBJECT) {
                throw new SearchParseException(context, "Unexpected token " + token + " in [reducers]: reductions array must contain reduction definition objects.");
            }

            token = parser.nextToken();
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new SearchParseException(context, "Unexpected token " + token + " in [reducers]: reductions definitions must start with the name of the reduction.");
            }
            final String reductionName = parser.currentName();
            if (!validReducerMatcher.reset(reductionName).matches()) {
                throw new SearchParseException(context, "Invalid ruduction name [" + reductionName + "]. Reduction names must be alpha-numeric and can only contain '_' and '-'");
            }

            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new SearchParseException(context, "Reduction definition for [" + reductionName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_ARRAY + "].");
            }

            ReducerFactory factory = null;
            ReducerFactories subFactories = null;

            Map<String, Object> metaData = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new SearchParseException(context, "Expected [" + XContentParser.Token.FIELD_NAME + "] under a [" + XContentParser.Token.START_OBJECT + "], but got a [" + token + "] in [" + reductionName + "]");
                }
                final String fieldName = parser.currentName();

                token = parser.nextToken();
                if (token == XContentParser.Token.START_ARRAY) {
                    if ("reducers".equals(fieldName)) {
                        if (subFactories != null) {
                            throw new SearchParseException(context, "Found two sub aggregation definitions under [" + reductionName + "]");
                        }
                        subFactories = parseReducers(parser, context, level+1);
                    } else {
                        throw new SearchParseException(context, "Expected [" + XContentParser.Token.START_ARRAY + "] for field [reducers], but got a [" + token + "] in [" + reductionName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("meta".equals(fieldName)) {
                        metaData = parser.map();
                    } else {
                        if (factory != null) {
                            throw new SearchParseException(context, "Found two reduction type definitions in [" + reductionName + "]: ["
                                    + factory.type + "] and [" + fieldName + "]");
                        }
                        Reducer.Parser reducerParser = parser(fieldName);
                        if (reducerParser == null) {
                            throw new SearchParseException(context, "Could not find reducer type [" + fieldName + "] in [" + reductionName
                                    + "]");
                        }
                        factory = reducerParser.parse(reductionName, parser, context);
                    }
                } else {
                    throw new SearchParseException(context, "Unexpected token [" + token + "] for field [" + fieldName + "] in [" + reductionName + "]");
                }
            }

            token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new SearchParseException(context, "Unexpected token " + token + " in [reducers]: expected " + Token.END_OBJECT + ".");
            }

            if (factory == null) {
                throw new SearchParseException(context, "Missing definition for reduction [" + reductionName + "]");
            }

            if (metaData != null) {
                factory.setMetaData(metaData);
            }

            if (subFactories != null) {
                factory.subFactories(subFactories);
            }

            if (level == 0) {
                factory.validate();
            }

            factories.add(factory);
        }

        return factories.build();
    }

}
