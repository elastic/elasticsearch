/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.SuggestingErrorOnUnknown;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
 * Internally, a {@link RetrieverBuilder} is just a wrapper for other search
 * elements that are extracted into a {@link SearchSourceBuilder}. The advantage
 * retrievers have is in the API they appear as a tree-like structure enabling
 * easier reasoning about what a search does.
 *
 * This is the base class for all other retrievers. This class does not support
 * serialization and is expected to be fully extracted to a {@link SearchSourceBuilder}
 * prior to any transport calls.
 */
public abstract class RetrieverBuilder implements ToXContent {

    public static final NodeFeature RETRIEVERS_SUPPORTED = new NodeFeature("retrievers_supported");

    public static final ParseField PRE_FILTER_FIELD = new ParseField("filter");

    protected static void declareBaseParserFields(
        String name,
        AbstractObjectParser<? extends RetrieverBuilder, RetrieverParserContext> parser
    ) {
        parser.declareObjectArray((r, v) -> r.preFilterQueryBuilders = v, (p, c) -> {
            QueryBuilder preFilterQueryBuilder = AbstractQueryBuilder.parseTopLevelQuery(p, c::trackQueryUsage);
            c.trackSectionUsage(name + ":" + PRE_FILTER_FIELD.getPreferredName());
            return preFilterQueryBuilder;
        }, PRE_FILTER_FIELD);
    }

    /**
     * This method parsers a top-level retriever within a search and tracks its own depth. Currently, the
     * maximum depth allowed is limited to 2 as a compound retriever cannot currently contain another
     * compound retriever.
     */
    public static RetrieverBuilder parseTopLevelRetrieverBuilder(XContentParser parser, RetrieverParserContext context) throws IOException {
        parser = new FilterXContentParserWrapper(parser) {

            int nestedDepth = 0;

            @Override
            public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
                if (categoryClass.equals(RetrieverBuilder.class)) {
                    nestedDepth++;

                    if (nestedDepth > 2) {
                        throw new IllegalArgumentException(
                            "the nested depth of the [" + name + "] retriever exceeds the maximum nested depth [2] for retrievers"
                        );
                    }
                }

                T namedObject = getXContentRegistry().parseNamedObject(categoryClass, name, this, context);

                if (categoryClass.equals(RetrieverBuilder.class)) {
                    nestedDepth--;
                }

                return namedObject;
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

        context.trackSectionUsage(retrieverName);

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

    /**
     * Gets the filters for this retriever.
     */
    public List<QueryBuilder> getPreFilterQueryBuilders() {
        return preFilterQueryBuilders;
    }

    /**
     * This method is called at the end of parsing on behalf of a {@link SearchSourceBuilder}.
     * Elements from retrievers are expected to be "extracted" into the {@link SearchSourceBuilder}.
     */
    public abstract void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed);

    // ---- FOR TESTING XCONTENT PARSING ----

    public abstract String getName();

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (preFilterQueryBuilders.isEmpty() == false) {
            builder.field(PRE_FILTER_FIELD.getPreferredName(), preFilterQueryBuilders);
        }
        doToXContent(builder, params);
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
        return Objects.equals(preFilterQueryBuilders, that.preFilterQueryBuilders) && doEquals(o);
    }

    protected abstract boolean doEquals(Object o);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), preFilterQueryBuilders, doHashCode());
    }

    protected abstract int doHashCode();

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    // ---- END FOR TESTING ----
}
