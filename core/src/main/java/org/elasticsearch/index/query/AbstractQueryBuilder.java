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

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for all classes producing lucene queries.
 * Supports conversion to BytesReference and creation of lucene Query objects.
 */
public abstract class AbstractQueryBuilder<QB extends AbstractQueryBuilder<QB>> extends ToXContentToBytes implements QueryBuilder {

    /** Default for boost to apply to resulting Lucene query. Defaults to 1.0*/
    public static final float DEFAULT_BOOST = 1.0f;
    public static final ParseField NAME_FIELD = new ParseField("_name");
    public static final ParseField BOOST_FIELD = new ParseField("boost");

    protected String queryName;
    protected float boost = DEFAULT_BOOST;

    protected AbstractQueryBuilder() {
        super(XContentType.JSON);
    }

    protected AbstractQueryBuilder(StreamInput in) throws IOException {
        boost = in.readFloat();
        queryName = in.readOptionalString();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(boost);
        out.writeOptionalString(queryName);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    protected void printBoostAndQueryName(XContentBuilder builder) throws IOException {
        builder.field(BOOST_FIELD.getPreferredName(), boost);
        if (queryName != null) {
            builder.field(NAME_FIELD.getPreferredName(), queryName);
        }
    }

    @Override
    public final Query toQuery(QueryShardContext context) throws IOException {
        Query query = doToQuery(context);
        if (query != null) {
            if (boost != DEFAULT_BOOST) {
                if (query instanceof SpanQuery) {
                    query = new SpanBoostQuery((SpanQuery) query, boost);
                } else {
                    query = new BoostQuery(query, boost);
                }
            }
            if (queryName != null) {
                context.addNamedQuery(queryName, query);
            }
        }
        return query;
    }

    @Override
    public final Query toFilter(QueryShardContext context) throws IOException {
        Query result = null;
            final boolean originalIsFilter = context.isFilter();
            try {
                context.setIsFilter(true);
                result = toQuery(context);
            } finally {
                context.setIsFilter(originalIsFilter);
            }
        return result;
    }

    protected abstract Query doToQuery(QueryShardContext context) throws IOException;

    /**
     * Sets the query name for the query.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final QB queryName(String queryName) {
        this.queryName = queryName;
        return (QB) this;
    }

    /**
     * Returns the query name for the query.
     */
    @Override
    public final String queryName() {
        return queryName;
    }

    /**
     * Returns the boost for this query.
     */
    @Override
    public final float boost() {
        return this.boost;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final QB boost(float boost) {
        this.boost = boost;
        return (QB) this;
    }

    protected final QueryValidationException addValidationError(String validationError, QueryValidationException validationException) {
        return QueryValidationException.addValidationError(getName(), validationError, validationException);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        QB other = (QB) obj;
        return Objects.equals(queryName, other.queryName) &&
                Objects.equals(boost, other.boost) &&
                doEquals(other);
    }

    /**
     * Indicates whether some other {@link QueryBuilder} object of the same type is "equal to" this one.
     */
    protected abstract boolean doEquals(QB other);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), queryName, boost, doHashCode());
    }

    protected abstract int doHashCode();

    /**
     * This helper method checks if the object passed in is a string, if so it
     * converts it to a {@link BytesRef}.
     * @param obj the input object
     * @return the same input object or a {@link BytesRef} representation if input was of type string
     */
    protected static Object convertToBytesRefIfString(Object obj) {
        if (obj instanceof String) {
            return BytesRefs.toBytesRef(obj);
        }
        return obj;
    }

    /**
     * This helper method checks if the object passed in is a {@link BytesRef}, if so it
     * converts it to a utf8 string.
     * @param obj the input object
     * @return the same input object or a utf8 string if input was of type {@link BytesRef}
     */
    protected static Object convertToStringIfBytesRef(Object obj) {
        if (obj instanceof BytesRef) {
            return ((BytesRef) obj).utf8ToString();
        }
        return obj;
    }

    /**
     * Helper method to convert collection of {@link QueryBuilder} instances to lucene
     * {@link Query} instances. {@link QueryBuilder} that return <tt>null</tt> calling
     * their {@link QueryBuilder#toQuery(QueryShardContext)} method are not added to the
     * resulting collection.
     */
    protected static Collection<Query> toQueries(Collection<QueryBuilder> queryBuilders, QueryShardContext context) throws QueryShardException,
            IOException {
        List<Query> queries = new ArrayList<>(queryBuilders.size());
        for (QueryBuilder queryBuilder : queryBuilders) {
            Query query = queryBuilder.toQuery(context);
            if (query != null) {
                queries.add(query);
            }
        }
        return queries;
    }

    @Override
    public String getName() {
        //default impl returns the same as writeable name, but we keep the distinction between the two just to make sure
        return getWriteableName();
    }

    protected final static void writeQueries(StreamOutput out, List<? extends QueryBuilder> queries) throws IOException {
        out.writeVInt(queries.size());
        for (QueryBuilder query : queries) {
            out.writeNamedWriteable(query);
        }
    }

    protected final static List<QueryBuilder> readQueries(StreamInput in) throws IOException {
        List<QueryBuilder> queries = new ArrayList<>();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            queries.add(in.readNamedWriteable(QueryBuilder.class));
        }
        return queries;
    }

    @Override
    public final QueryBuilder rewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder rewritten = doRewrite(queryShardContext);
        if (rewritten == this) {
            return rewritten;
        }
        if (queryName() != null && rewritten.queryName() == null) { // we inherit the name
            rewritten.queryName(queryName());
        }
        if (boost() != DEFAULT_BOOST && rewritten.boost() == DEFAULT_BOOST) {
            rewritten.boost(boost());
        }
        return rewritten;
    }

    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        return this;
    }

    /**
     * For internal usage only!
     *
     * Extracts the inner hits from the query tree.
     * While it extracts inner hits, child inner hits are inlined into the inner hit builder they belong to.
     */
    protected void extractInnerHitBuilders(Map<String, InnerHitBuilder> innerHits) {
    }

    // Like Objects.requireNotNull(...) but instead throws a IllegalArgumentException
    protected static <T> T requireValue(T value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }
}
