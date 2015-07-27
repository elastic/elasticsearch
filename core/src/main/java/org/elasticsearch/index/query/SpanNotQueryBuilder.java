/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SpanNotQueryBuilder extends AbstractQueryBuilder<SpanNotQueryBuilder> implements SpanQueryBuilder<SpanNotQueryBuilder> {

    public static final String NAME = "span_not";

    /** the default pre parameter size */
    public static final int DEFAULT_PRE = 0;
    /** the default post parameter size */
    public static final int DEFAULT_POST = 0;

    private final SpanQueryBuilder include;

    private final SpanQueryBuilder exclude;

    private int pre = DEFAULT_PRE;

    private int post = DEFAULT_POST;

    static final SpanNotQueryBuilder PROTOTYPE = new SpanNotQueryBuilder(null, null);

    /**
     * Construct a span query matching spans from <code>include</code> which
     * have no overlap with spans from <code>exclude</code>.
     * @param include the span query whose matches are filtered
     * @param exclude the span query whose matches must not overlap
     */
    public SpanNotQueryBuilder(SpanQueryBuilder include, SpanQueryBuilder exclude) {
        this.include = include;
        this.exclude = exclude;
    }

    /**
     * @return the span query whose matches are filtered
     */
    public SpanQueryBuilder include() {
        return this.include;
    }

    /**
     * @return the span query whose matches must not overlap
     */
    public SpanQueryBuilder exclude() {
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
        builder.field("include");
        include.toXContent(builder, params);
        builder.field("exclude");
        exclude.toXContent(builder, params);
        builder.field("pre", pre);
        builder.field("post", post);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {

        Query includeQuery = this.include.toQuery(context);
        assert includeQuery instanceof SpanQuery;
        Query excludeQuery = this.exclude.toQuery(context);
        assert excludeQuery instanceof SpanQuery;

        SpanNotQuery query = new SpanNotQuery((SpanQuery) includeQuery, (SpanQuery) excludeQuery, pre, post);
        return query;
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (include == null) {
            validationException = addValidationError("inner clause [include] cannot be null.", validationException);
        } else {
            validationException = validateInnerQuery(include, validationException);
        }
        if (exclude == null) {
            validationException = addValidationError("inner clause [exclude] cannot be null.", validationException);
        } else {
            validationException = validateInnerQuery(exclude, validationException);
        }
        return validationException;
    }

    @Override
    protected SpanNotQueryBuilder doReadFrom(StreamInput in) throws IOException {
        SpanQueryBuilder include = in.readNamedWriteable();
        SpanQueryBuilder exclude = in.readNamedWriteable();
        SpanNotQueryBuilder queryBuilder = new SpanNotQueryBuilder(include, exclude);
        queryBuilder.pre(in.readVInt());
        queryBuilder.post(in.readVInt());
        return queryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(include);
        out.writeNamedWriteable(exclude);
        out.writeVInt(pre);
        out.writeVInt(post);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(include, exclude, pre, post);
    }

    @Override
    protected boolean doEquals(SpanNotQueryBuilder other) {
        return Objects.equals(include, other.include) &&
               Objects.equals(exclude, other.exclude) &&
               (pre == other.pre) &&
               (post == other.post);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
