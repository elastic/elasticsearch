/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LinearCombinationRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "linear_combination";
    public static final NodeFeature NODE_FEATURE = new NodeFeature(NAME + "_retriever");

    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");

    public static final ObjectParser<LinearCombinationRetrieverBuilder, RetrieverParserContext> PARSER =
        new ObjectParser<>(
            NAME,
            LinearCombinationRetrieverBuilder::new
        );

    static {
        PARSER.declareObjectArray((r, v) -> r.retrieverBuilders = v, (p, c) -> {
            p.nextToken();
            String name = p.currentName();
            RetrieverBuilder retrieverBuilder = p.namedObject(RetrieverBuilder.class, name, c);
            p.nextToken();
            return retrieverBuilder;
        }, RETRIEVERS_FIELD);

        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static LinearCombinationRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context)
        throws IOException {
        if (context.clusterSupportsFeature(NODE_FEATURE) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        return PARSER.apply(parser, context);
    }

    private List<RetrieverBuilder> retrieverBuilders = Collections.emptyList();

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        for (RetrieverBuilder retrieverBuilder : retrieverBuilders) {
            if (preFilterQueryBuilders.isEmpty() == false) {
                retrieverBuilder.getPreFilterQueryBuilders().addAll(preFilterQueryBuilders);
            }

            retrieverBuilder.extractToSearchSourceBuilder(searchSourceBuilder, true);
        }
    }

    @Override
    public String getName() {
        return "";
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) { }

    @Override
    protected boolean doEquals(Object o) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }
}
