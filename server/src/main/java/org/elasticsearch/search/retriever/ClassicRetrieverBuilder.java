/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ClassicRetrieverBuilder extends RetrieverBuilder<ClassicRetrieverBuilder> {

    public static final String NAME = "classic";

    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField SEARCH_AFTER_FIELD = new ParseField("search_after");
    public static final ParseField TERMINATE_AFTER_FIELD = new ParseField("terminate_after");
    public static final ParseField SORT_FIELD = new ParseField("sort");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");
    public static final ParseField POST_FILTER_FIELD = new ParseField("post_filter");
    public static final ParseField RESCORE_FIELD = new ParseField("rescore");
    public static final ParseField COLLAPSE_FIELD = new ParseField("collapse");

    public static final ObjectParser<ClassicRetrieverBuilder, RetrieverParserContext> PARSER = new ObjectParser<>(
        NAME,
        ClassicRetrieverBuilder::new
    );

    static {
        PARSER.declareObject(ClassicRetrieverBuilder::queryBuilder, (p, c) -> {
            QueryBuilder queryBuilder = AbstractQueryBuilder.parseTopLevelQuery(p, c::trackQueryUsage);
            c.trackSectionUsage(NAME + ":" + QUERY_FIELD.getPreferredName());
            return queryBuilder;
        }, QUERY_FIELD);
        PARSER.declareObject(ClassicRetrieverBuilder::searchAfterBuilder, (p, c) -> {
            SearchAfterBuilder searchAfterBuilder = SearchAfterBuilder.fromXContent(p);
            c.trackSectionUsage(NAME + ":" + SEARCH_AFTER_FIELD.getPreferredName());
            return searchAfterBuilder;
        }, SEARCH_AFTER_FIELD);
        PARSER.declareObject(ClassicRetrieverBuilder::terminateAfter, (p, c) -> {
            int terminateAfter = p.intValue();
            c.trackSectionUsage(NAME + ":" + TERMINATE_AFTER_FIELD.getPreferredName());
            return terminateAfter;
        }, TERMINATE_AFTER_FIELD);
        PARSER.declareObject(ClassicRetrieverBuilder::sortBuilders, (p, c) -> {
            List<SortBuilder<?>> sortBuilders = SortBuilder.fromXContent(p);
            c.trackSectionUsage(NAME + ":" + SORT_FIELD.getPreferredName());
            return sortBuilders;
        }, SORT_FIELD);
        PARSER.declareObject(ClassicRetrieverBuilder::minScore, (p, c) -> {
            float minScore = p.floatValue();
            c.trackSectionUsage(NAME + ":" + MIN_SCORE_FIELD.getPreferredName());
            return minScore;
        }, MIN_SCORE_FIELD);
        PARSER.declareObject(ClassicRetrieverBuilder::queryBuilder, (p, c) -> {
            QueryBuilder postFilterQueryBuilder = AbstractQueryBuilder.parseTopLevelQuery(p, c::trackQueryUsage);
            c.trackSectionUsage(NAME + ":" + POST_FILTER_FIELD.getPreferredName());
            return postFilterQueryBuilder;
        }, POST_FILTER_FIELD);
        PARSER.declareObject(ClassicRetrieverBuilder::rescorerBuilders, (p, c) -> {
            @SuppressWarnings("rawtypes")
            List<RescorerBuilder> rescorerBuilders = new ArrayList<>();
            if (p.currentToken() == XContentParser.Token.START_ARRAY) {
                while ((p.nextToken()) != XContentParser.Token.END_ARRAY) {
                    rescorerBuilders.add(RescorerBuilder.parseFromXContent(p, c::trackRescorerUsage));
                }
            } else {
                rescorerBuilders.add(RescorerBuilder.parseFromXContent(p, c::trackRescorerUsage));
            }
            c.trackSectionUsage(NAME + ":" + RESCORE_FIELD.getPreferredName());
            return rescorerBuilders;
        }, RESCORE_FIELD);

        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static ClassicRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }

    private QueryBuilder queryBuilder;
    private SearchAfterBuilder searchAfterBuilder;
    private int terminateAfter = SearchContext.DEFAULT_TERMINATE_AFTER;
    private List<SortBuilder<?>> sortBuilders;
    private Float minScore;
    private QueryBuilder postFilterQueryBuilder;
    @SuppressWarnings("rawtypes")
    private List<RescorerBuilder> rescorerBuilders;
    private CollapseBuilder collapseBuilder;

    public ClassicRetrieverBuilder() {

    }

    public ClassicRetrieverBuilder(ClassicRetrieverBuilder original) {
        super(original);
        queryBuilder = original.queryBuilder;
        searchAfterBuilder = original.searchAfterBuilder;
        terminateAfter = original.terminateAfter;
        sortBuilders = original.sortBuilders;
        minScore = original.minScore;
        postFilterQueryBuilder = original.postFilterQueryBuilder;
        rescorerBuilders = original.rescorerBuilders;
        collapseBuilder = original.collapseBuilder;
    }

    @SuppressWarnings("unchecked")
    public ClassicRetrieverBuilder(StreamInput in) throws IOException {
        super(in);
        queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        searchAfterBuilder = in.readOptionalWriteable(SearchAfterBuilder::new);
        terminateAfter = in.readVInt();
        if (in.readBoolean()) {
            sortBuilders = (List<SortBuilder<?>>) (Object) in.readNamedWriteableCollectionAsList(SortBuilder.class);
        }
        minScore = in.readOptionalFloat();
        postFilterQueryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        if (in.readBoolean()) {
            rescorerBuilders = in.readNamedWriteableCollectionAsList(RescorerBuilder.class);
        }
        collapseBuilder = in.readOptionalWriteable(CollapseBuilder::new);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(queryBuilder);
        out.writeOptionalWriteable(searchAfterBuilder);
        out.writeVInt(terminateAfter);
        if (sortBuilders == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeNamedWriteableCollection(sortBuilders);
        }
        out.writeOptionalFloat(minScore);
        out.writeOptionalNamedWriteable(postFilterQueryBuilder);
        if (rescorerBuilders == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeNamedWriteableCollection(rescorerBuilders);
        }
        out.writeOptionalWriteable(collapseBuilder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.RETRIEVERS_ADDED;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        if (queryBuilder != null) {
            builder.field(QUERY_FIELD.getPreferredName(), queryBuilder);
        }

        if (searchAfterBuilder != null) {
            builder.array(SEARCH_AFTER_FIELD.getPreferredName(), searchAfterBuilder.getSortValues());
        }

        if (terminateAfter != SearchContext.DEFAULT_TERMINATE_AFTER) {
            builder.field(TERMINATE_AFTER_FIELD.getPreferredName(), terminateAfter);
        }

        if (sortBuilders != null) {
            builder.startArray(SORT_FIELD.getPreferredName());

            for (SortBuilder<?> sortBuilder : sortBuilders) {
                sortBuilder.toXContent(builder, params);
            }

            builder.endArray();
        }

        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }

        if (postFilterQueryBuilder != null) {
            builder.field(POST_FILTER_FIELD.getPreferredName(), postFilterQueryBuilder);
        }

        if (rescorerBuilders != null) {
            builder.startArray(RESCORE_FIELD.getPreferredName());

            for (RescorerBuilder<?> rescorerBuilder : rescorerBuilders) {
                rescorerBuilder.toXContent(builder, params);
            }

            builder.endArray();
        }

        if (collapseBuilder != null) {
            builder.field(COLLAPSE_FIELD.getPreferredName(), collapseBuilder);
        }
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ClassicRetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        ClassicRetrieverBuilder crb = super.rewrite(ctx);

        QueryBuilder queryBuilder = this.queryBuilder == null ? null : this.queryBuilder.rewrite(ctx);
        List<SortBuilder<?>> sortBuilders = this.sortBuilders == null ? null : Rewriteable.rewrite(this.sortBuilders, ctx);
        QueryBuilder postFilterQueryBuilder = this.postFilterQueryBuilder == null ? null : this.postFilterQueryBuilder.rewrite(ctx);
        List<RescorerBuilder> rescorerBuilders = this.rescorerBuilders == null ? null : Rewriteable.rewrite(this.rescorerBuilders, ctx);

        if (queryBuilder != this.queryBuilder
            || sortBuilders != this.sortBuilders
            || postFilterQueryBuilder != this.postFilterQueryBuilder
            || rescorerBuilders != this.rescorerBuilders) {

            if (crb == this) {
                crb = shallowCopyInstance();
            }

            crb.queryBuilder = queryBuilder;
            crb.sortBuilders = sortBuilders;
            crb.postFilterQueryBuilder = postFilterQueryBuilder;
            crb.rescorerBuilders = rescorerBuilders;
        }

        return crb;
    }

    @Override
    protected ClassicRetrieverBuilder shallowCopyInstance() {
        return new ClassicRetrieverBuilder(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        ClassicRetrieverBuilder that = (ClassicRetrieverBuilder) o;
        return terminateAfter == that.terminateAfter
            && Objects.equals(queryBuilder, that.queryBuilder)
            && Objects.equals(searchAfterBuilder, that.searchAfterBuilder)
            && Objects.equals(sortBuilders, that.sortBuilders)
            && Objects.equals(minScore, that.minScore)
            && Objects.equals(postFilterQueryBuilder, that.postFilterQueryBuilder)
            && Objects.equals(rescorerBuilders, that.rescorerBuilders)
            && Objects.equals(collapseBuilder, that.collapseBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            queryBuilder,
            searchAfterBuilder,
            terminateAfter,
            sortBuilders,
            minScore,
            postFilterQueryBuilder,
            rescorerBuilders,
            collapseBuilder
        );
    }

    public QueryBuilder queryBuilder() {
        return queryBuilder;
    }

    public ClassicRetrieverBuilder queryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    public SearchAfterBuilder searchAfterBuilder() {
        return searchAfterBuilder;
    }

    public ClassicRetrieverBuilder searchAfterBuilder(SearchAfterBuilder searchAfterBuilder) {
        this.searchAfterBuilder = searchAfterBuilder;
        return this;
    }

    public int terminateAfter() {
        return terminateAfter;
    }

    public ClassicRetrieverBuilder terminateAfter(int terminateAfter) {
        this.terminateAfter = terminateAfter;
        return this;
    }

    public List<SortBuilder<?>> sortBuilders() {
        return sortBuilders;
    }

    public ClassicRetrieverBuilder sortBuilders(List<SortBuilder<?>> sortBuilders) {
        this.sortBuilders = sortBuilders;
        return this;
    }

    public Float minScore() {
        return minScore;
    }

    public ClassicRetrieverBuilder minScore(Float minScore) {
        this.minScore = minScore;
        return this;
    }

    public QueryBuilder postFilterQueryBuilder() {
        return queryBuilder;
    }

    public ClassicRetrieverBuilder postFilterQueryBuilder(QueryBuilder postFilterQueryBuilder) {
        this.postFilterQueryBuilder = postFilterQueryBuilder;
        return this;
    }

    @SuppressWarnings("rawtypes")
    public List<RescorerBuilder> rescorerBuilders() {
        return rescorerBuilders;
    }

    @SuppressWarnings("rawtypes")
    public ClassicRetrieverBuilder rescorerBuilders(List<RescorerBuilder> rescorerBuilders) {
        this.rescorerBuilders = rescorerBuilders;
        return this;
    }

    public CollapseBuilder collapseBuilder() {
        return collapseBuilder;
    }

    public ClassicRetrieverBuilder collapseBuilder(CollapseBuilder collapseBuilder) {
        this.collapseBuilder = collapseBuilder;
        return this;
    }

    public void doExtractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        if (queryBuilder != null) {
            searchSourceBuilder.subSearches().add(new SubSearchSourceBuilder(queryBuilder));
        }

        if (searchSourceBuilder.searchAfter() == null) {
            if (searchAfterBuilder != null) {
                searchSourceBuilder.searchAfter(searchAfterBuilder.getSortValues());
            }
        } else {
            throw new IllegalStateException("[search_after] cannot be declared as a retriever value and as a global value");
        }

        if (searchSourceBuilder.terminateAfter() == SearchContext.DEFAULT_TERMINATE_AFTER) {
            searchSourceBuilder.terminateAfter(terminateAfter);
        } else {
            throw new IllegalStateException("[terminate_after] cannot be declared as a retriever value and as a global value");
        }

        if (searchSourceBuilder.sorts() == null) {
            if (sortBuilders != null) {
                searchSourceBuilder.sort(sortBuilders);
            }
        } else {
            throw new IllegalStateException("[sort] cannot be declared as a retriever value and as a global value");
        }

        if (searchSourceBuilder.minScore() == null) {
            if (minScore != null) {
                searchSourceBuilder.minScore(minScore);
            }
        } else {
            throw new IllegalStateException("[min_score] cannot be declared as a retriever value and as a global value");
        }

        if (searchSourceBuilder.postFilter() == null) {
            searchSourceBuilder.postFilter(postFilterQueryBuilder);
        } else {
            throw new IllegalStateException("[post_filter] cannot be declared as a retriever value and as a global value");
        }

        if (searchSourceBuilder.rescores() == null) {
            if (rescorerBuilders != null) {
                for (RescorerBuilder<?> rescorerBuilder : rescorerBuilders) {
                    searchSourceBuilder.addRescorer(rescorerBuilder);
                }
            }
        } else {
            throw new IllegalStateException("[rescore] cannot be declared as a retriever value and as a global value");
        }

        if (searchSourceBuilder.collapse() == null) {
            if (collapseBuilder != null) {
                searchSourceBuilder.collapse(collapseBuilder);
            }
        } else {
            throw new IllegalStateException("[collapse] cannot be declared as a retriever value and as a global value");
        }
    }
}
