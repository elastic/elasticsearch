/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanNotQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.query.SpanQueryBuilder.SpanQueryBuilderUtil.checkNoBoost;

public class SpanNotQueryBuilder extends AbstractQueryBuilder<SpanNotQueryBuilder> implements SpanQueryBuilder {
    public static final String NAME = "span_not";

    /** the default pre parameter size */
    public static final int DEFAULT_PRE = 0;
    /** the default post parameter size */
    public static final int DEFAULT_POST = 0;

    private static final ParseField POST_FIELD = new ParseField("post");
    private static final ParseField PRE_FIELD = new ParseField("pre");
    private static final ParseField DIST_FIELD = new ParseField("dist");
    private static final ParseField EXCLUDE_FIELD = new ParseField("exclude");
    private static final ParseField INCLUDE_FIELD = new ParseField("include");

    private final SpanQueryBuilder include;

    private final SpanQueryBuilder exclude;

    private int pre = DEFAULT_PRE;

    private int post = DEFAULT_POST;

    /**
     * Construct a span query matching spans from <code>include</code> which
     * have no overlap with spans from <code>exclude</code>.
     * @param include the span query whose matches are filtered
     * @param exclude the span query whose matches must not overlap
     */
    public SpanNotQueryBuilder(SpanQueryBuilder include, SpanQueryBuilder exclude) {
        if (include == null) {
            throw new IllegalArgumentException("inner clause [include] cannot be null.");
        }
        if (exclude == null) {
            throw new IllegalArgumentException("inner clause [exclude] cannot be null.");
        }
        this.include = include;
        this.exclude = exclude;
    }

    /**
     * Read from a stream.
     */
    public SpanNotQueryBuilder(StreamInput in) throws IOException {
        super(in);
        include = (SpanQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
        exclude = (SpanQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
        pre = in.readVInt();
        post = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(include);
        out.writeNamedWriteable(exclude);
        out.writeVInt(pre);
        out.writeVInt(post);
    }

    /**
     * @return the span query whose matches are filtered
     */
    public SpanQueryBuilder includeQuery() {
        return this.include;
    }

    /**
     * @return the span query whose matches must not overlap
     */
    public SpanQueryBuilder excludeQuery() {
        return this.exclude;
    }

    /**
     * @param dist the amount of tokens from within the include span can’t have overlap with the exclude span.
     * Equivalent to setting both pre and post parameter.
     */
    public SpanNotQueryBuilder dist(int dist) {
        pre(dist);
        post(dist);
        return this;
    }

    /**
     * @param pre the amount of tokens before the include span that can’t have overlap with the exclude span. Values
     * smaller than 0 will be ignored and 0 used instead.
     */
    public SpanNotQueryBuilder pre(int pre) {
        this.pre = (pre >= 0) ? pre : 0;
        return this;
    }

    /**
     * @return the amount of tokens before the include span that can’t have overlap with the exclude span.
     * @see SpanNotQueryBuilder#pre(int)
     */
    public Integer pre() {
        return this.pre;
    }

    /**
     * @param post the amount of tokens after the include span that can’t have overlap with the exclude span.
     */
    public SpanNotQueryBuilder post(int post) {
        this.post = (post >= 0) ? post : 0;
        return this;
    }

    /**
     * @return the amount of tokens after the include span that can’t have overlap with the exclude span.
     * @see SpanNotQueryBuilder#post(int)
     */
    public Integer post() {
        return this.post;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(INCLUDE_FIELD.getPreferredName());
        include.toXContent(builder, params);
        builder.field(EXCLUDE_FIELD.getPreferredName());
        exclude.toXContent(builder, params);
        builder.field(PRE_FIELD.getPreferredName(), pre);
        builder.field(POST_FIELD.getPreferredName(), post);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static SpanNotQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        SpanQueryBuilder include = null;
        SpanQueryBuilder exclude = null;

        Integer dist = null;
        Integer pre = null;
        Integer post = null;

        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (INCLUDE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "span_not [include] must be of type span query");
                    }
                    include = (SpanQueryBuilder) query;
                    checkNoBoost(NAME, currentFieldName, parser, include);
                } else if (EXCLUDE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "span_not [exclude] must be of type span query");
                    }
                    exclude = (SpanQueryBuilder) query;
                    checkNoBoost(NAME, currentFieldName, parser, exclude);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_not] query does not support [" + currentFieldName + "]");
                }
            } else {
                if (DIST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    dist = parser.intValue();
                } else if (PRE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    pre = parser.intValue();
                } else if (POST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    post = parser.intValue();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_not] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (include == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_not must have [include] span query clause");
        }
        if (exclude == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_not must have [exclude] span query clause");
        }
        if (dist != null && (pre != null || post != null)) {
            throw new ParsingException(parser.getTokenLocation(), "span_not can either use [dist] or [pre] & [post] (or none)");
        }

        SpanNotQueryBuilder spanNotQuery = new SpanNotQueryBuilder(include, exclude);
        if (dist != null) {
            spanNotQuery.dist(dist);
        }
        if (pre != null) {
            spanNotQuery.pre(pre);
        }
        if (post != null) {
            spanNotQuery.post(post);
        }
        spanNotQuery.boost(boost);
        spanNotQuery.queryName(queryName);
        return spanNotQuery;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {

        Query includeQuery = this.include.toQuery(context);
        assert includeQuery instanceof SpanQuery;
        Query excludeQuery = this.exclude.toQuery(context);
        assert excludeQuery instanceof SpanQuery;

        return new SpanNotQuery((SpanQuery) includeQuery, (SpanQuery) excludeQuery, pre, post);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(include, exclude, pre, post);
    }

    @Override
    protected boolean doEquals(SpanNotQueryBuilder other) {
        return Objects.equals(include, other.include)
            && Objects.equals(exclude, other.exclude)
            && (pre == other.pre)
            && (post == other.post);
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
