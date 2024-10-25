/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.SuggestingErrorOnUnknown;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * A retriever represents an API element that returns an ordered list of top
 * documents. These can be obtained from a query, from another retriever, etc.
 * Internally, a {@link RetrieverBuilder} is first rewritten into its simplest
 * form and then its elements are extracted into a {@link SearchSourceBuilder}.
 *
 * The advantage retrievers have is in the API they appear as a tree-like structure enabling
 * easier reasoning about what a search does.
 *
 * This is the base class for all other retrievers. This class does not support
 * serialization and is expected to be fully extracted to a {@link SearchSourceBuilder}
 * prior to any transport calls.
 */
public abstract class RetrieverBuilder implements Rewriteable<RetrieverBuilder>, ToXContent {

    public static final NodeFeature RETRIEVERS_SUPPORTED = new NodeFeature("retrievers_supported");

    public static final ParseField PRE_FILTER_FIELD = new ParseField("filter");

    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");

    public static final ParseField NAME_FIELD = new ParseField("_name");

    protected static void declareBaseParserFields(
        String name,
        AbstractObjectParser<? extends RetrieverBuilder, RetrieverParserContext> parser
    ) {
        parser.declareObjectArray(
            (r, v) -> r.preFilterQueryBuilders = v,
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p, c::trackQueryUsage),
            PRE_FILTER_FIELD
        );
        parser.declareString(RetrieverBuilder::retrieverName, NAME_FIELD);
        parser.declareFloat(RetrieverBuilder::minScore, MIN_SCORE_FIELD);
    }

    public RetrieverBuilder retrieverName(String retrieverName) {
        this.retrieverName = retrieverName;
        return this;
    }

    public RetrieverBuilder minScore(Float minScore) {
        this.minScore = minScore;
        return this;
    }

    public static RetrieverBuilder parseTopLevelRetrieverBuilder(XContentParser parser, RetrieverParserContext context) throws IOException {
        parser = new FilterXContentParserWrapper(parser) {

            @Override
            public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
                return getXContentRegistry().parseNamedObject(categoryClass, name, this, context);
            }
        };

        return parseInnerRetrieverBuilder(parser, context);
    }

    protected static RetrieverBuilder parseInnerRetrieverBuilder(XContentParser parser, RetrieverParserContext context) throws IOException {
        Objects.requireNonNull(context);

        if (parser.currentToken() != XContentParser.Token.START_OBJECT && parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "retriever malformed, must start with [" + XContentParser.Token.START_OBJECT + "]"
            );
        }

        if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "retriever malformed, empty clause found");
        }

        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "retriever malformed, no field after [" + XContentParser.Token.START_OBJECT + "]"
            );
        }

        String retrieverName = parser.currentName();

        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + retrieverName + "] retriever malformed, no [" + XContentParser.Token.START_OBJECT + "] after retriever name"
            );
        }

        RetrieverBuilder retrieverBuilder;

        try {
            retrieverBuilder = parser.namedObject(RetrieverBuilder.class, retrieverName, context);
        } catch (NamedObjectNotFoundException nonfe) {
            String message = String.format(
                Locale.ROOT,
                "unknown retriever [%s]%s",
                retrieverName,
                SuggestingErrorOnUnknown.suggest(retrieverName, nonfe.getCandidates())
            );

            throw new ParsingException(new XContentLocation(nonfe.getLineNumber(), nonfe.getColumnNumber()), message, nonfe);
        }

        context.trackRetrieverUsage(retrieverName);

        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "["
                    + retrieverName
                    + "] malformed retriever, expected ["
                    + XContentParser.Token.END_OBJECT
                    + "] but found ["
                    + parser.currentToken()
                    + "]"
            );
        }

        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "["
                    + retrieverName
                    + "] malformed retriever, expected ["
                    + XContentParser.Token.END_OBJECT
                    + "] but found ["
                    + parser.currentToken()
                    + "]"
            );
        }

        return retrieverBuilder;
    }

    protected List<QueryBuilder> preFilterQueryBuilders = new ArrayList<>();

    protected String retrieverName;

    protected Float minScore;

    /**
     * Determines if this retriever contains sub-retrievers that need to be executed prior to search.
     */
    public boolean isCompound() {
        return false;
    }

    protected RankDoc[] rankDocs = null;

    public RetrieverBuilder() {}

    protected final List<QueryBuilder> rewritePreFilters(QueryRewriteContext ctx) throws IOException {
        List<QueryBuilder> newFilters = new ArrayList<>(preFilterQueryBuilders.size());
        boolean changed = false;
        for (var filter : preFilterQueryBuilders) {
            var newFilter = filter.rewrite(ctx);
            changed |= filter != newFilter;
            newFilters.add(newFilter);
        }
        if (changed) {
            return newFilters;
        }
        return preFilterQueryBuilders;
    }

    /**
     * This function is called by compound {@link RetrieverBuilder} to return the original query that
     * was used by this retriever to compute its top documents.
     */
    public abstract QueryBuilder topDocsQuery();

    public QueryBuilder explainQuery() {
        return topDocsQuery();
    }

    public Float minScore() {
        return minScore;
    }

    public void setRankDocs(RankDoc[] rankDocs) {
        this.rankDocs = rankDocs;
    }

    /**
     * Gets the filters for this retriever.
     */
    public List<QueryBuilder> getPreFilterQueryBuilders() {
        return preFilterQueryBuilders;
    }

    @Override
    public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        return this;
    }

    /**
     * This method is called at the end of rewriting on behalf of a {@link SearchSourceBuilder}.
     * Elements from retrievers are expected to be "extracted" into the {@link SearchSourceBuilder}.
     */
    public abstract void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed);

    public ActionRequestValidationException validate(
        SearchSourceBuilder source,
        ActionRequestValidationException validationException,
        boolean isScroll,
        boolean allowPartialSearchResults
    ) {
        return validationException;
    }

    // ---- FOR TESTING XCONTENT PARSING ----

    public abstract String getName();

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startObject(getName());
        if (preFilterQueryBuilders.isEmpty() == false) {
            builder.field(PRE_FILTER_FIELD.getPreferredName(), preFilterQueryBuilders);
        }
        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
        if (retrieverName != null) {
            builder.field(NAME_FIELD.getPreferredName(), retrieverName);
        }
        doToXContent(builder, params);
        builder.endObject();
        builder.endObject();

        return builder;
    }

    protected abstract void doToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException;

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RetrieverBuilder that = (RetrieverBuilder) o;
        return Objects.equals(preFilterQueryBuilders, that.preFilterQueryBuilders)
            && Objects.equals(minScore, that.minScore)
            && doEquals(o);
    }

    protected abstract boolean doEquals(Object o);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), preFilterQueryBuilders, minScore, doHashCode());
    }

    protected abstract int doHashCode();

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public String retrieverName() {
        return retrieverName;
    }

    // ---- END FOR TESTING ----
}
