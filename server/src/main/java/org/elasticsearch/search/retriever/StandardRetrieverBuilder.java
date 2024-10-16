/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A standard retriever is used to represent anything that is a query along
 * with some elements to specify parameters for that query.
 */
public final class StandardRetrieverBuilder extends RetrieverBuilder implements ToXContent {

    public static final String NAME = "standard";
    public static final NodeFeature STANDARD_RETRIEVER_SUPPORTED = new NodeFeature("standard_retriever_supported");

    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField SEARCH_AFTER_FIELD = new ParseField("search_after");
    public static final ParseField TERMINATE_AFTER_FIELD = new ParseField("terminate_after");
    public static final ParseField SORT_FIELD = new ParseField("sort");
    public static final ParseField COLLAPSE_FIELD = new ParseField("collapse");

    public static final ObjectParser<StandardRetrieverBuilder, RetrieverParserContext> PARSER = new ObjectParser<>(
        NAME,
        StandardRetrieverBuilder::new
    );

    static {
        PARSER.declareObject((r, v) -> r.queryBuilder = v, (p, c) -> {
            QueryBuilder queryBuilder = AbstractQueryBuilder.parseTopLevelQuery(p, c::trackQueryUsage);
            return queryBuilder;
        }, QUERY_FIELD);

        PARSER.declareField(
            (r, v) -> r.searchAfterBuilder = v,
            (p, c) -> SearchAfterBuilder.fromXContent(p),
            SEARCH_AFTER_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareField((r, v) -> r.terminateAfter = v, (p, c) -> p.intValue(), TERMINATE_AFTER_FIELD, ObjectParser.ValueType.INT);
        PARSER.declareField(
            (r, v) -> r.sortBuilders = v,
            (p, c) -> SortBuilder.fromXContent(p),
            SORT_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareField(
            (r, v) -> r.collapseBuilder = v,
            (p, c) -> CollapseBuilder.fromXContent(p),
            COLLAPSE_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static StandardRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(STANDARD_RETRIEVER_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        return PARSER.apply(parser, context);
    }

    QueryBuilder queryBuilder;
    SearchAfterBuilder searchAfterBuilder;
    int terminateAfter = SearchContext.DEFAULT_TERMINATE_AFTER;
    List<SortBuilder<?>> sortBuilders;
    CollapseBuilder collapseBuilder;

    public StandardRetrieverBuilder() {}

    public StandardRetrieverBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    private StandardRetrieverBuilder(StandardRetrieverBuilder clone) {
        this.retrieverName = clone.retrieverName;
        this.queryBuilder = clone.queryBuilder;
        this.minScore = clone.minScore;
        this.sortBuilders = clone.sortBuilders;
        this.preFilterQueryBuilders = clone.preFilterQueryBuilders;
        this.collapseBuilder = clone.collapseBuilder;
        this.searchAfterBuilder = clone.searchAfterBuilder;
        this.terminateAfter = clone.terminateAfter;
    }

    @Override
    public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        boolean changed = false;
        List<SortBuilder<?>> rewrittenSortBuilders = null;
        if (sortBuilders != null) {
            rewrittenSortBuilders = new ArrayList<>(sortBuilders.size());
            for (var sort : sortBuilders) {
                var newSort = sort.rewrite(ctx);
                rewrittenSortBuilders.add(newSort);
                changed |= newSort != sort;
            }
        }
        var rewrittenFilters = rewritePreFilters(ctx);
        changed |= rewrittenFilters != preFilterQueryBuilders;

        QueryBuilder rewrittenQuery = null;
        if (queryBuilder != null) {
            rewrittenQuery = queryBuilder.rewrite(ctx);
            changed |= rewrittenQuery != queryBuilder;
        }

        if (changed) {
            var rewritten = new StandardRetrieverBuilder(this);
            rewritten.sortBuilders = rewrittenSortBuilders;
            rewritten.preFilterQueryBuilders = rewrittenFilters;
            rewritten.queryBuilder = rewrittenQuery;
            return rewritten;
        }
        return this;
    }

    @Override
    public QueryBuilder topDocsQuery() {
        if (preFilterQueryBuilders.isEmpty()) {
            QueryBuilder qb = queryBuilder;
            qb.queryName(this.retrieverName);
            return qb;
        }
        var ret = new BoolQueryBuilder().filter(queryBuilder).queryName(this.retrieverName);
        preFilterQueryBuilders.stream().forEach(ret::filter);
        return ret;
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        if (preFilterQueryBuilders.isEmpty() == false || minScore != null) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

            for (QueryBuilder preFilterQueryBuilder : preFilterQueryBuilders) {
                boolQueryBuilder.filter(preFilterQueryBuilder);
            }

            if (queryBuilder != null) {
                boolQueryBuilder.must(queryBuilder);
            }
            searchSourceBuilder.subSearches().add(new SubSearchSourceBuilder(boolQueryBuilder));
        } else if (queryBuilder != null) {
            searchSourceBuilder.subSearches().add(new SubSearchSourceBuilder(queryBuilder));
        }

        if (searchAfterBuilder != null) {
            if (compoundUsed) {
                throw new IllegalArgumentException(
                    "[" + SEARCH_AFTER_FIELD.getPreferredName() + "] cannot be used in children of compound retrievers"
                );
            }

            searchSourceBuilder.searchAfter(searchAfterBuilder.getSortValues());
        }

        if (terminateAfter != SearchContext.DEFAULT_TERMINATE_AFTER) {
            if (compoundUsed) {
                throw new IllegalArgumentException(
                    "[" + TERMINATE_AFTER_FIELD.getPreferredName() + "] cannot be used in children of compound retrievers"
                );
            }

            searchSourceBuilder.terminateAfter(terminateAfter);
        }

        if (sortBuilders != null) {
            searchSourceBuilder.sort(sortBuilders);
        }

        if (minScore != null) {
            searchSourceBuilder.minScore(minScore);
        }

        if (collapseBuilder != null) {
            searchSourceBuilder.collapse(collapseBuilder);
        }
    }

    // ---- FOR TESTING XCONTENT PARSING ----

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void doToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (queryBuilder != null) {
            builder.field(QUERY_FIELD.getPreferredName(), queryBuilder);
        }

        if (searchAfterBuilder != null) {
            searchAfterBuilder.innerToXContent(builder);
        }

        if (terminateAfter != SearchContext.DEFAULT_TERMINATE_AFTER) {
            builder.field(TERMINATE_AFTER_FIELD.getPreferredName(), terminateAfter);
        }

        if (sortBuilders != null) {
            builder.field(SORT_FIELD.getPreferredName(), sortBuilders);
        }

        if (collapseBuilder != null) {
            builder.field(COLLAPSE_FIELD.getPreferredName(), collapseBuilder);
        }
    }

    @Override
    public boolean doEquals(Object o) {
        StandardRetrieverBuilder that = (StandardRetrieverBuilder) o;
        return terminateAfter == that.terminateAfter
            && Objects.equals(queryBuilder, that.queryBuilder)
            && Objects.equals(searchAfterBuilder, that.searchAfterBuilder)
            && Objects.equals(sortBuilders, that.sortBuilders)
            && Objects.equals(collapseBuilder, that.collapseBuilder);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(queryBuilder, searchAfterBuilder, terminateAfter, sortBuilders, collapseBuilder);
    }

    // ---- END FOR TESTING ----
}
