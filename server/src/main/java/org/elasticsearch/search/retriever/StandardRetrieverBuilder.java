/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public final class StandardRetrieverBuilder extends RetrieverBuilder<StandardRetrieverBuilder> {

    public static final String NAME = "standard";

    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField SEARCH_AFTER_FIELD = new ParseField("search_after");
    public static final ParseField TERMINATE_AFTER_FIELD = new ParseField("terminate_after");
    public static final ParseField SORT_FIELD = new ParseField("sort");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");
    public static final ParseField COLLAPSE_FIELD = new ParseField("collapse");

    public static final ObjectParser<StandardRetrieverBuilder, RetrieverParserContext> PARSER = new ObjectParser<>(
        NAME,
        StandardRetrieverBuilder::new
    );

    static {
        PARSER.declareObject(StandardRetrieverBuilder::queryBuilder, (p, c) -> {
            QueryBuilder queryBuilder = AbstractQueryBuilder.parseTopLevelQuery(p, c::trackQueryUsage);
            c.trackSectionUsage(NAME + ":" + QUERY_FIELD.getPreferredName());
            return queryBuilder;
        }, QUERY_FIELD);

        PARSER.declareField(StandardRetrieverBuilder::searchAfterBuilder, (p, c) -> {
            SearchAfterBuilder searchAfterBuilder = SearchAfterBuilder.fromXContent(p);
            c.trackSectionUsage(NAME + ":" + SEARCH_AFTER_FIELD.getPreferredName());
            return searchAfterBuilder;
        }, SEARCH_AFTER_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);

        PARSER.declareField(StandardRetrieverBuilder::terminateAfter, (p, c) -> {
            int terminateAfter = p.intValue();
            c.trackSectionUsage(NAME + ":" + TERMINATE_AFTER_FIELD.getPreferredName());
            return terminateAfter;
        }, TERMINATE_AFTER_FIELD, ObjectParser.ValueType.INT);

        PARSER.declareField(StandardRetrieverBuilder::sortBuilders, (p, c) -> {
            List<SortBuilder<?>> sortBuilders = SortBuilder.fromXContent(p);
            c.trackSectionUsage(NAME + ":" + SORT_FIELD.getPreferredName());
            return sortBuilders;
        }, SORT_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);

        PARSER.declareField(StandardRetrieverBuilder::minScore, (p, c) -> {
            float minScore = p.floatValue();
            c.trackSectionUsage(NAME + ":" + MIN_SCORE_FIELD.getPreferredName());
            return minScore;
        }, MIN_SCORE_FIELD, ObjectParser.ValueType.FLOAT);

        PARSER.declareField(StandardRetrieverBuilder::collapseBuilder, (p, c) -> {
            CollapseBuilder collapseBuilder = CollapseBuilder.fromXContent(p);
            if (collapseBuilder.getField() != null) {
                c.trackSectionUsage(COLLAPSE_FIELD.getPreferredName());
            }
            return collapseBuilder;
        }, COLLAPSE_FIELD, ObjectParser.ValueType.OBJECT);

        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static StandardRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }

    private QueryBuilder queryBuilder;
    private SearchAfterBuilder searchAfterBuilder;
    private int terminateAfter = SearchContext.DEFAULT_TERMINATE_AFTER;
    private List<SortBuilder<?>> sortBuilders;
    private Float minScore;
    private CollapseBuilder collapseBuilder;

    public QueryBuilder queryBuilder() {
        return queryBuilder;
    }

    public StandardRetrieverBuilder queryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    public SearchAfterBuilder searchAfterBuilder() {
        return searchAfterBuilder;
    }

    public StandardRetrieverBuilder searchAfterBuilder(SearchAfterBuilder searchAfterBuilder) {
        this.searchAfterBuilder = searchAfterBuilder;
        return this;
    }

    public int terminateAfter() {
        return terminateAfter;
    }

    public StandardRetrieverBuilder terminateAfter(int terminateAfter) {
        this.terminateAfter = terminateAfter;
        return this;
    }

    public List<SortBuilder<?>> sortBuilders() {
        return sortBuilders;
    }

    public StandardRetrieverBuilder sortBuilders(List<SortBuilder<?>> sortBuilders) {
        this.sortBuilders = sortBuilders;
        return this;
    }

    public Float minScore() {
        return minScore;
    }

    public StandardRetrieverBuilder minScore(Float minScore) {
        this.minScore = minScore;
        return this;
    }

    public CollapseBuilder collapseBuilder() {
        return collapseBuilder;
    }

    public StandardRetrieverBuilder collapseBuilder(CollapseBuilder collapseBuilder) {
        this.collapseBuilder = collapseBuilder;
        return this;
    }

    public void doExtractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        if (preFilterQueryBuilders().isEmpty() == false) {
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

        if (searchSourceBuilder.searchAfter() == null) {
            if (searchAfterBuilder != null) {
                searchSourceBuilder.searchAfter(searchAfterBuilder.getSortValues());
            }
        } else {
            throw new IllegalArgumentException("[search_after] cannot be declared on multiple retrievers");
        }

        if (searchSourceBuilder.terminateAfter() == SearchContext.DEFAULT_TERMINATE_AFTER) {
            searchSourceBuilder.terminateAfter(terminateAfter);
        } else {
            throw new IllegalArgumentException("[terminate_after] cannot be declared on multiple retrievers");
        }

        if (searchSourceBuilder.sorts() == null) {
            if (sortBuilders != null) {
                searchSourceBuilder.sort(sortBuilders);
            }
        } else {
            throw new IllegalArgumentException("[sort] cannot be declared on multiple retrievers");
        }

        if (searchSourceBuilder.minScore() == null) {
            if (minScore != null) {
                searchSourceBuilder.minScore(minScore);
            }
        } else {
            throw new IllegalArgumentException("[min_score] cannot be declared on multiple retrievers");
        }

        if (searchSourceBuilder.collapse() == null) {
            if (collapseBuilder != null) {
                searchSourceBuilder.collapse(collapseBuilder);
            }
        } else {
            throw new IllegalArgumentException("[collapse] cannot be declared on multiple retrievers");
        }
    }
}
