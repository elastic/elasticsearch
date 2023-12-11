/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

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
        return PARSER.apply(parser, context);
    }

    private List<? extends RetrieverBuilder<?>> retrieverBuilders = Collections.emptyList();
    private int windowSize = RRFRankBuilder.DEFAULT_WINDOW_SIZE;
    private int rankConstant = RRFRankBuilder.DEFAULT_RANK_CONSTANT;

    public RRFRetrieverBuilder() {

    }

    public RRFRetrieverBuilder(RRFRetrieverBuilder original) {
        super(original);
        retrieverBuilders = original.retrieverBuilders;
        windowSize = original.windowSize;
        rankConstant = original.rankConstant;
    }

    @SuppressWarnings("unchecked")
    public RRFRetrieverBuilder(StreamInput in) throws IOException {
        super(in);
        retrieverBuilders = (List<RetrieverBuilder<?>>) (Object) in.readNamedWriteableCollectionAsList(RetrieverBuilder.class);
        windowSize = in.readVInt();
        rankConstant = in.readVInt();
    }

    @Override
    public String getWriteableName() {
        return RRFRankPlugin.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.RETRIEVERS_ADDED;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableCollection(retrieverBuilders);
        out.writeVInt(windowSize);
        out.writeVInt(rankConstant);
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            builder.startArray(RETRIEVERS_FIELD.getPreferredName());
            retrieverBuilder.toXContent(builder, params);
            builder.endArray();
        }

        builder.field(WINDOW_SIZE_FIELD.getPreferredName(), windowSize);
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
    }

    @Override
    protected RRFRetrieverBuilder shallowCopyInstance() {
        return new RRFRetrieverBuilder(this);
    }

    @Override
    public QueryBuilder buildDfsQuery() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            QueryBuilder queryBuilder = retrieverBuilder.buildDfsQuery();

            if (queryBuilder != null) {
                boolQueryBuilder.should(queryBuilder);
            }
        }

        return boolQueryBuilder.should().isEmpty() ? null : boolQueryBuilder;
    }

    @Override
    public void doBuildDfsSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            retrieverBuilder.doBuildDfsSearchSourceBuilder(searchSourceBuilder);
        }
    }

    @Override
    public boolean hasDfsKnnResults() {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            if (retrieverBuilder.hasDfsKnnResults()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void doProcessDfsSearchResults(List<DfsSearchResult> dfsSearchResults, List<DfsKnnResults> dfsKnnResults) {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            retrieverBuilder.doProcessDfsSearchResults(dfsSearchResults, dfsKnnResults);
        }
    }

    /*@Override
    public void doExtractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            retrieverBuilder.doExtractToSearchSourceBuilder(searchSourceBuilder);
        }

        if (searchSourceBuilder.rankBuilder() == null) {
            searchSourceBuilder.rankBuilder(new RRFRankBuilder(windowSize, rankConstant));
        } else {
            throw new IllegalStateException("[rank] cannot be declared as a retriever value and as a global value");
        }
    }*/
}
