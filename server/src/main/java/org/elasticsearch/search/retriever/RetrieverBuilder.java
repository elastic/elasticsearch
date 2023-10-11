/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.xcontent.SuggestingErrorOnUnknown;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public abstract class RetrieverBuilder<RB extends RetrieverBuilder<RB>>
    implements
        VersionedNamedWriteable,
        ToXContentObject,
        Rewriteable<RB> {

    public static final ParseField PRE_FILTER_FIELD = new ParseField("filter");
    public static final ParseField _NAME_FIELD = new ParseField("_name");

    protected static void declareBaseParserFields(
        String name,
        AbstractObjectParser<? extends RetrieverBuilder<?>, RetrieverParserContext> parser
    ) {
        parser.declareObject(RetrieverBuilder::preFilterQueryBuilder, (p, c) -> {
            QueryBuilder preFilterQueryBuilder = AbstractQueryBuilder.parseTopLevelQuery(p, c::trackQueryUsage);
            c.trackSectionUsage(name + ":" + PRE_FILTER_FIELD.getPreferredName());
            return preFilterQueryBuilder;
        }, PRE_FILTER_FIELD);
        parser.declareString(RetrieverBuilder::_name, _NAME_FIELD);
    }

    public static RetrieverBuilder<?> parseTopLevelRetrieverBuilder(XContentParser parser, RetrieverParserContext context)
        throws IOException {
        parser = new FilterXContentParserWrapper(parser) {

            int nestedDepth = 0;

            @Override
            public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
                if (categoryClass.equals(QueryBuilder.class)) {
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

    protected static RetrieverBuilder<?> parseInnerRetrieverBuilder(XContentParser parser, RetrieverParserContext context)
        throws IOException {
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

        RetrieverBuilder<?> retrieverBuilder;

        try {
            retrieverBuilder = parser.namedObject(RetrieverBuilder.class, retrieverName, context);
            context.trackSectionUsage(retrieverName);
        } catch (NamedObjectNotFoundException nonfe) {
            String message = String.format(
                Locale.ROOT,
                "unknown retriever [%s]%s",
                retrieverName,
                SuggestingErrorOnUnknown.suggest(retrieverName, nonfe.getCandidates())
            );

            throw new ParsingException(new XContentLocation(nonfe.getLineNumber(), nonfe.getColumnNumber()), message, nonfe);
        }

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

    protected QueryBuilder preFilterQueryBuilder;
    protected String _name;

    public RetrieverBuilder() {

    }

    public RetrieverBuilder(RetrieverBuilder<?> original) {
        preFilterQueryBuilder = original.preFilterQueryBuilder;
        _name = original._name;
    }

    public RetrieverBuilder(StreamInput in) throws IOException {
        preFilterQueryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        _name = in.readOptionalString();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(preFilterQueryBuilder);
        out.writeOptionalString(_name);
        doWriteTo(out);
    }

    public abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (preFilterQueryBuilder != null) {
            builder.field(PRE_FILTER_FIELD.getPreferredName(), preFilterQueryBuilder);
        }
        if (_name != null) {
            builder.field(_NAME_FIELD.getPreferredName(), _name);
        }
        doToXContent(builder, params);
        builder.endObject();

        return builder;
    }

    protected abstract void doToXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    @SuppressWarnings("unchecked")
    public RB rewrite(QueryRewriteContext ctx) throws IOException {
        if (preFilterQueryBuilder != null) {
            QueryBuilder rewrittenFilter = preFilterQueryBuilder.rewrite(ctx);

            if (rewrittenFilter != preFilterQueryBuilder) {
                return shallowCopyInstance().preFilterQueryBuilder(preFilterQueryBuilder);
            }
        }

        return (RB) this;
    }

    protected abstract RB shallowCopyInstance();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RetrieverBuilder<?> that = (RetrieverBuilder<?>) o;
        return Objects.equals(preFilterQueryBuilder, that.preFilterQueryBuilder) && Objects.equals(_name, that._name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(preFilterQueryBuilder, _name);
    }

    public QueryBuilder preFilterQueryBuilder() {
        return preFilterQueryBuilder;
    }

    @SuppressWarnings("unchecked")
    public RB preFilterQueryBuilder(QueryBuilder preFilter) {
        this.preFilterQueryBuilder = preFilter;
        return (RB) this;
    }

    public String _name() {
        return _name;
    }

    @SuppressWarnings("unchecked")
    public RB _name(String _name) {
        this._name = _name;
        return (RB) this;
    }

    public final void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        doExtractToSearchSourceBuilder(searchSourceBuilder);

        if (preFilterQueryBuilder != null) {
            throw new IllegalStateException("[filter] is not supported");
        }

        if (_name != null) {
            throw new IllegalStateException("[_name] is not supported");
        }
    }

    public abstract void doExtractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder);
}
