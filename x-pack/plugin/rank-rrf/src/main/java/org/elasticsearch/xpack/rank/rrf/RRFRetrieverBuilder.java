/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.rank.rrf.RRFRankPlugin.NAME;

/**
 * An rrf retriever is used to represent an rrf rank element, but
 * as a tree-like structure. This retriever is a compound retriever
 * meaning it has a set of child retrievers that each return a set of
 * top docs that will then be combined and ranked according to the rrf
 * formula.
 */
public final class RRFRetrieverBuilder extends RetrieverBuilder {

    public static final NodeFeature RRF_RETRIEVER_SUPPORTED = new NodeFeature("rrf_retriever_supported");

    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");
    public static final ParseField RANK_WINDOW_SIZE_FIELD = new ParseField("rank_window_size");
    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");

    public static final ObjectParser<RRFRetrieverBuilder, RetrieverParserContext> PARSER = new ObjectParser<>(
        NAME,
        RRFRetrieverBuilder::new
    );

    static {
        PARSER.declareObjectArray((r, v) -> r.retrieverBuilders = v, (p, c) -> {
            p.nextToken();
            String name = p.currentName();
            RetrieverBuilder retrieverBuilder = p.namedObject(RetrieverBuilder.class, name, c);
            p.nextToken();
            return retrieverBuilder;
        }, RETRIEVERS_FIELD);
        PARSER.declareInt((r, v) -> r.rankWindowSize = v, RANK_WINDOW_SIZE_FIELD);
        PARSER.declareInt((r, v) -> r.rankConstant = v, RANK_CONSTANT_FIELD);

        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static RRFRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(RRF_RETRIEVER_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        if (RRFRankPlugin.RANK_RRF_FEATURE.check(XPackPlugin.getSharedLicenseState()) == false) {
            throw LicenseUtils.newComplianceException("Reciprocal Rank Fusion (RRF)");
        }
        return PARSER.apply(parser, context);
    }

    List<RetrieverBuilder> retrieverBuilders = Collections.emptyList();
    int rankWindowSize = RRFRankBuilder.DEFAULT_RANK_WINDOW_SIZE;
    int rankConstant = RRFRankBuilder.DEFAULT_RANK_CONSTANT;

    @Override
    public QueryBuilder topDocsQuery() {
        throw new IllegalStateException("{" + getName() + "} cannot be nested");
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        if (compoundUsed) {
            throw new IllegalArgumentException("[rank] cannot be used in children of compound retrievers");
        }

        for (RetrieverBuilder retrieverBuilder : retrieverBuilders) {
            if (preFilterQueryBuilders.isEmpty() == false) {
                retrieverBuilder.getPreFilterQueryBuilders().addAll(preFilterQueryBuilders);
            }

            retrieverBuilder.extractToSearchSourceBuilder(searchSourceBuilder, true);
        }

        searchSourceBuilder.rankBuilder(new RRFRankBuilder(rankWindowSize, rankConstant));
    }

    // ---- FOR TESTING XCONTENT PARSING ----

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        if (retrieverBuilders.isEmpty() == false) {
            builder.startArray(RETRIEVERS_FIELD.getPreferredName());

            for (RetrieverBuilder retrieverBuilder : retrieverBuilders) {
                builder.startObject();
                builder.field(retrieverBuilder.getName());
                retrieverBuilder.toXContent(builder, params);
                builder.endObject();
            }

            builder.endArray();
        }

        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
    }

    @Override
    public boolean doEquals(Object o) {
        RRFRetrieverBuilder that = (RRFRetrieverBuilder) o;
        return rankWindowSize == that.rankWindowSize
            && rankConstant == that.rankConstant
            && Objects.equals(retrieverBuilders, that.retrieverBuilders);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(retrieverBuilders, rankWindowSize, rankConstant);
    }

    // ---- END FOR TESTING ----
}
