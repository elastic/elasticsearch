/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public final class RRFRetrieverBuilder extends RetrieverBuilder<RRFRetrieverBuilder> {

    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");
    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");
    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");

    public static final ObjectParser<RRFRetrieverBuilder, RetrieverParserContext> PARSER = new ObjectParser<>(
        RRFRankPlugin.NAME,
        RRFRetrieverBuilder::new
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
        PARSER.declareInt((b, v) -> b.rankConstant = v, RANK_CONSTANT_FIELD);

        RetrieverBuilder.declareBaseParserFields(RRFRankPlugin.NAME, PARSER);
    }

    public static RRFRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (RRFRankPlugin.RANK_RRF_FEATURE.check(XPackPlugin.getSharedLicenseState()) == false) {
            throw LicenseUtils.newComplianceException("Reciprocal Rank Fusion (RRF)");
        }
        return PARSER.apply(parser, context);
    }

    private List<? extends RetrieverBuilder<?>> retrieverBuilders = Collections.emptyList();
    private int windowSize = RRFRankBuilder.DEFAULT_WINDOW_SIZE;
    private int rankConstant = RRFRankBuilder.DEFAULT_RANK_CONSTANT;

    @Override
    public void doExtractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            if (preFilterQueryBuilders.isEmpty() == false) {
                retrieverBuilder.preFilterQueryBuilders().addAll(preFilterQueryBuilders);
            }

            retrieverBuilder.doExtractToSearchSourceBuilder(searchSourceBuilder);
        }

        if (searchSourceBuilder.rankBuilder() == null) {
            searchSourceBuilder.rankBuilder(new RRFRankBuilder(windowSize, rankConstant));
        } else {
            throw new IllegalStateException("[rank] cannot be declared on multiple retrievers");
        }
    }
}
