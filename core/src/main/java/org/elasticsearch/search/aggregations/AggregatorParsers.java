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
package org.elasticsearch.search.aggregations;

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A registry for all the aggregator parser, also servers as the main parser for the aggregations module
 */
public class AggregatorParsers {

    public static final Pattern VALID_AGG_NAME = Pattern.compile("[^\\[\\]>]+");
    private final ImmutableMap<String, Aggregator.Parser> aggParsers;
    private final ImmutableMap<String, PipelineAggregator.Parser> pipelineAggregatorParsers;


    /**
     * Constructs the AggregatorParsers out of all the given parsers
     *
     * @param aggParsers
     *            The available aggregator parsers (dynamically injected by the
     *            {@link org.elasticsearch.search.SearchModule}
     *            ).
     */
    @Inject
    public AggregatorParsers(Set<Aggregator.Parser> aggParsers, Set<PipelineAggregator.Parser> pipelineAggregatorParsers) {
        MapBuilder<String, Aggregator.Parser> aggParsersBuilder = MapBuilder.newMapBuilder();
        for (Aggregator.Parser parser : aggParsers) {
            aggParsersBuilder.put(parser.type(), parser);
        }
        this.aggParsers = aggParsersBuilder.immutableMap();
        MapBuilder<String, PipelineAggregator.Parser> pipelineAggregatorParsersBuilder = MapBuilder.newMapBuilder();
        for (PipelineAggregator.Parser parser : pipelineAggregatorParsers) {
            pipelineAggregatorParsersBuilder.put(parser.type(), parser);
        }
        this.pipelineAggregatorParsers = pipelineAggregatorParsersBuilder.immutableMap();
    }

    /**
     * Returns the parser that is registered under the given aggregation type.
     *
     * @param type  The aggregation type
     * @return      The parser associated with the given aggregation type.
     */
    public Aggregator.Parser parser(String type) {
        return aggParsers.get(type);
    }

    /**
     * Returns the parser that is registered under the given pipeline aggregator
     * type.
     *
     * @param type
     *            The pipeline aggregator type
     * @return The parser associated with the given pipeline aggregator type.
     */
    public PipelineAggregator.Parser pipelineAggregator(String type) {
        return pipelineAggregatorParsers.get(type);
    }

    /**
     * Parses the aggregation request recursively generating aggregator factories in turn.
     *
     * @param parser    The input xcontent that will be parsed.
     * @param context   The search context.
     *
     * @return          The parsed aggregator factories.
     *
     * @throws IOException When parsing fails for unknown reasons.
     */
    public AggregatorFactories parseAggregators(XContentParser parser, SearchContext context) throws IOException {
        return parseAggregators(parser, context, 0);
    }


    private AggregatorFactories parseAggregators(XContentParser parser, SearchContext context, int level) throws IOException {
        Matcher validAggMatcher = VALID_AGG_NAME.matcher("");
        AggregatorFactories.Builder factories = new AggregatorFactories.Builder();

        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new SearchParseException(context, "Unexpected token " + token
                        + " in [aggs]: aggregations definitions must start with the name of the aggregation.", parser.getTokenLocation());
            }
            final String aggregationName = parser.currentName();
            if (!validAggMatcher.reset(aggregationName).matches()) {
                throw new SearchParseException(context, "Invalid aggregation name [" + aggregationName
                        + "]. Aggregation names must be alpha-numeric and can only contain '_' and '-'", parser.getTokenLocation());
            }

            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new SearchParseException(context, "Aggregation definition for [" + aggregationName + " starts with a [" + token
                        + "], expected a [" + XContentParser.Token.START_OBJECT + "].", parser.getTokenLocation());
            }

            AggregatorFactory aggFactory = null;
            PipelineAggregatorFactory pipelineAggregatorFactory = null;
            AggregatorFactories subFactories = null;

            Map<String, Object> metaData = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new SearchParseException(context, "Expected [" + XContentParser.Token.FIELD_NAME + "] under a ["
                            + XContentParser.Token.START_OBJECT + "], but got a [" + token + "] in [" + aggregationName + "]",
                            parser.getTokenLocation());
                }
                final String fieldName = parser.currentName();

                token = parser.nextToken();
                if ("aggregations_binary".equals(fieldName)) {
                    if (subFactories != null) {
                        throw new SearchParseException(context, "Found two sub aggregation definitions under [" + aggregationName + "]",
                                parser.getTokenLocation());
                    }
                    XContentParser binaryParser = null;
                    if (token == XContentParser.Token.VALUE_STRING || token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                        byte[] source = parser.binaryValue();
                        binaryParser = XContentFactory.xContent(source).createParser(source);
                    } else {
                        throw new SearchParseException(context, "Expected [" + XContentParser.Token.VALUE_STRING + " or "
                                + XContentParser.Token.VALUE_EMBEDDED_OBJECT + "] for [" + fieldName + "], but got a [" + token + "] in ["
                                + aggregationName + "]", parser.getTokenLocation());
                    }
                    XContentParser.Token binaryToken = binaryParser.nextToken();
                    if (binaryToken != XContentParser.Token.START_OBJECT) {
                        throw new SearchParseException(context, "Expected [" + XContentParser.Token.START_OBJECT
                                + "] as first token when parsing [" + fieldName + "], but got a [" + binaryToken + "] in ["
                                + aggregationName + "]", parser.getTokenLocation());
                    }
                    subFactories = parseAggregators(binaryParser, context, level + 1);
                } else if (token == XContentParser.Token.START_OBJECT) {
                    switch (fieldName) {
                    case "meta":
                        metaData = parser.map();
                        break;
                    case "aggregations":
                    case "aggs":
                        if (subFactories != null) {
                            throw new SearchParseException(context,
                                    "Found two sub aggregation definitions under [" + aggregationName + "]", parser.getTokenLocation());
                        }
                        subFactories = parseAggregators(parser, context, level + 1);
                        break;
                    default:
                        if (aggFactory != null) {
                            throw new SearchParseException(context, "Found two aggregation type definitions in [" + aggregationName
                                    + "]: [" + aggFactory.type + "] and [" + fieldName + "]", parser.getTokenLocation());
                        }
                        if (pipelineAggregatorFactory != null) {
                            throw new SearchParseException(context, "Found two aggregation type definitions in [" + aggregationName
                                    + "]: [" + pipelineAggregatorFactory + "] and [" + fieldName + "]", parser.getTokenLocation());
                        }

                        Aggregator.Parser aggregatorParser = parser(fieldName);
                        if (aggregatorParser == null) {
                            PipelineAggregator.Parser pipelineAggregatorParser = pipelineAggregator(fieldName);
                            if (pipelineAggregatorParser == null) {
                                throw new SearchParseException(context, "Could not find aggregator type [" + fieldName + "] in ["
                                        + aggregationName + "]", parser.getTokenLocation());
                            } else {
                                pipelineAggregatorFactory = pipelineAggregatorParser.parse(aggregationName, parser, context);
                            }
                        } else {
                            aggFactory = aggregatorParser.parse(aggregationName, parser, context);
                        }
                    }
                } else {
                    throw new SearchParseException(context, "Expected [" + XContentParser.Token.START_OBJECT + "] under [" + fieldName
                            + "], but got a [" + token + "] in [" + aggregationName + "]", parser.getTokenLocation());
                }
            }

            if (aggFactory == null && pipelineAggregatorFactory == null) {
                throw new SearchParseException(context, "Missing definition for aggregation [" + aggregationName + "]",
                        parser.getTokenLocation());
            } else if (aggFactory != null) {
                assert pipelineAggregatorFactory == null;
            if (metaData != null) {
                    aggFactory.setMetaData(metaData);
            }

            if (subFactories != null) {
                    aggFactory.subFactories(subFactories);
            }

            if (level == 0) {
                    aggFactory.validate();
            }

                factories.addAggregator(aggFactory);
            } else {
                assert pipelineAggregatorFactory != null;
                if (subFactories != null) {
                    throw new SearchParseException(context, "Aggregation [" + aggregationName + "] cannot define sub-aggregations",
                            parser.getTokenLocation());
                }
                if (level == 0) {
                    pipelineAggregatorFactory
                            .validate(null, factories.getAggregatorFactories(), factories.getPipelineAggregatorFactories());
                }
                factories.addPipelineAggregator(pipelineAggregatorFactory);
            }
        }

        return factories.build();
    }

}
