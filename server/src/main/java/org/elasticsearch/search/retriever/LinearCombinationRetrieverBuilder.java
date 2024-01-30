/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public final class LinearCombinationRetrieverBuilder extends RetrieverBuilder<LinearCombinationRetrieverBuilder> {

    public static final String NAME = "linear_combination";

    public static final int DEFAULT_WINDOW_SIZE = SearchService.DEFAULT_SIZE;

    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");
    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    public static final ObjectParser<LinearCombinationRetrieverBuilder, RetrieverParserContext> PARSER = new ObjectParser<>(
        NAME,
        LinearCombinationRetrieverBuilder::new
    );

    static {
        PARSER.declareObjectArray((v, l) -> v.retrieverBuilders = l, (p, c) -> {
            p.nextToken();
            String name = p.currentName();
            RetrieverBuilder<?> retrieverBuilder = (RetrieverBuilder<?>) p.namedObject(RetrieverBuilder.class, name, c);
            p.nextToken();
            return retrieverBuilder;
        }, RETRIEVERS_FIELD);
        PARSER.declareInt((b, v) -> b.windowSize = v, WINDOW_SIZE_FIELD);

        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static LinearCombinationRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }

    private List<? extends RetrieverBuilder<?>> retrieverBuilders = Collections.emptyList();
    private int windowSize = DEFAULT_WINDOW_SIZE;

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            builder.startArray(RETRIEVERS_FIELD.getPreferredName());
            retrieverBuilder.toXContent(builder, params);
            builder.endArray();
        }

        builder.field(WINDOW_SIZE_FIELD.getPreferredName(), windowSize);
    }

    @Override
    public void doExtractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            if (preFilterQueryBuilders.isEmpty() == false) {
                retrieverBuilder.preFilterQueryBuilders().addAll(preFilterQueryBuilders);
            }

            retrieverBuilder.doExtractToSearchSourceBuilder(searchSourceBuilder);
        }
    }
}
