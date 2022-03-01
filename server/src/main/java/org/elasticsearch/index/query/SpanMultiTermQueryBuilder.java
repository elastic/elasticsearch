/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopTermsRewrite;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.SpanBooleanQueryRewriteWithMaxClause;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.lucene.queries.SpanMatchNoDocsQuery;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Query that allows wrapping a {@link MultiTermQueryBuilder} (one of wildcard, fuzzy, prefix, term, range or regexp query)
 * as a {@link SpanQueryBuilder} so it can be nested.
 */
public class SpanMultiTermQueryBuilder extends AbstractQueryBuilder<SpanMultiTermQueryBuilder> implements SpanQueryBuilder {

    public static final String NAME = "span_multi";
    private static final ParseField MATCH_FIELD = new ParseField("match");
    private final MultiTermQueryBuilder multiTermQueryBuilder;

    public SpanMultiTermQueryBuilder(MultiTermQueryBuilder multiTermQueryBuilder) {
        if (multiTermQueryBuilder == null) {
            throw new IllegalArgumentException("inner multi term query cannot be null");
        }
        this.multiTermQueryBuilder = multiTermQueryBuilder;
    }

    /**
     * Read from a stream.
     */
    public SpanMultiTermQueryBuilder(StreamInput in) throws IOException {
        super(in);
        multiTermQueryBuilder = (MultiTermQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(multiTermQueryBuilder);
    }

    public MultiTermQueryBuilder innerQuery() {
        return this.multiTermQueryBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(MATCH_FIELD.getPreferredName());
        multiTermQueryBuilder.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static SpanMultiTermQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        MultiTermQueryBuilder subQuery = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (query instanceof MultiTermQueryBuilder == false) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[span_multi] [" + MATCH_FIELD.getPreferredName() + "] must be of type multi term query"
                        );
                    }
                    subQuery = (MultiTermQueryBuilder) query;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_multi] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_multi] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (subQuery == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[span_multi] must have [" + MATCH_FIELD.getPreferredName() + "] multi term query clause"
            );
        }

        return new SpanMultiTermQueryBuilder(subQuery).queryName(queryName).boost(boost);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        // We do the rewrite in toQuery to not have to deal with the case when a multi-term builder rewrites to a non-multi-term
        // builder.
        QueryBuilder multiTermQueryBuilder = Rewriteable.rewrite(this.multiTermQueryBuilder, context);
        if (multiTermQueryBuilder instanceof MatchNoneQueryBuilder) {
            return new SpanMatchNoDocsQuery(this.multiTermQueryBuilder.fieldName(), "Inner query rewrote to match_none");
        } else if (multiTermQueryBuilder instanceof PrefixQueryBuilder prefixBuilder) {
            MappedFieldType fieldType = context.getFieldType(prefixBuilder.fieldName());
            if (fieldType == null) {
                throw new IllegalStateException("Rewrite first");
            }
            final SpanMultiTermQueryWrapper.SpanRewriteMethod spanRewriteMethod;
            if (prefixBuilder.rewrite() != null) {
                MultiTermQuery.RewriteMethod rewriteMethod = QueryParsers.parseRewriteMethod(
                    prefixBuilder.rewrite(),
                    null,
                    LoggingDeprecationHandler.INSTANCE
                );
                if (rewriteMethod instanceof TopTermsRewrite<?> innerRewrite) {
                    spanRewriteMethod = new SpanMultiTermQueryWrapper.TopTermsSpanBooleanQueryRewrite(innerRewrite.getSize());
                } else {
                    spanRewriteMethod = new SpanBooleanQueryRewriteWithMaxClause();
                }
            } else {
                spanRewriteMethod = new SpanBooleanQueryRewriteWithMaxClause();
            }
            return fieldType.spanPrefixQuery(prefixBuilder.value(), spanRewriteMethod, context);
        } else {
            Query subQuery = multiTermQueryBuilder.toQuery(context);
            while (true) {
                if (subQuery instanceof ConstantScoreQuery) {
                    subQuery = ((ConstantScoreQuery) subQuery).getQuery();
                } else if (subQuery instanceof BoostQuery boostQuery) {
                    subQuery = boostQuery.getQuery();
                } else {
                    break;
                }
            }
            if (subQuery instanceof MatchNoDocsQuery) {
                return new SpanMatchNoDocsQuery(this.multiTermQueryBuilder.fieldName(), subQuery.toString());
            } else if (subQuery instanceof MultiTermQuery == false) {
                throw new UnsupportedOperationException(
                    "unsupported inner query, should be " + MultiTermQuery.class.getName() + " but was " + subQuery.getClass().getName()
                );
            }
            MultiTermQuery multiTermQuery = (MultiTermQuery) subQuery;
            SpanMultiTermQueryWrapper<?> wrapper = new SpanMultiTermQueryWrapper<>(multiTermQuery);
            if (multiTermQuery.getRewriteMethod() instanceof TopTermsRewrite == false) {
                wrapper.setRewriteMethod(new SpanBooleanQueryRewriteWithMaxClause());
            }
            return wrapper;
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(multiTermQueryBuilder);
    }

    @Override
    protected boolean doEquals(SpanMultiTermQueryBuilder other) {
        return Objects.equals(multiTermQueryBuilder, other.multiTermQueryBuilder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }
}
