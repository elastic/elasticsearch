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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry.UnknownNamedObjectException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;

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
public abstract class AbstractQueryBuilder<QB extends AbstractQueryBuilder<QB>> implements QueryBuilder {

    /** Default for boost to apply to resulting Lucene query. Defaults to 1.0*/
    public static final float DEFAULT_BOOST = 1.0f;
    public static final ParseField NAME_FIELD = new ParseField("_name");
    public static final ParseField BOOST_FIELD = new ParseField("boost");

    protected String queryName;
    protected float boost = DEFAULT_BOOST;

    protected AbstractQueryBuilder() {

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
        Query result;
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
    static Object convertToBytesRefIfString(Object obj) {
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
    static Object convertToStringIfBytesRef(Object obj) {
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
    static Collection<Query> toQueries(Collection<QueryBuilder> queryBuilders, QueryShardContext context) throws QueryShardException,
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

    static void writeQueries(StreamOutput out, List<? extends QueryBuilder> queries) throws IOException {
        out.writeVInt(queries.size());
        for (QueryBuilder query : queries) {
            out.writeNamedWriteable(query);
        }
    }

    static List<QueryBuilder> readQueries(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<QueryBuilder> queries = new ArrayList<>(size);
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
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
    }

    /**
     * Parses a query excluding the query element that wraps it
     */
    public static QueryBuilder parseInnerQueryBuilder(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, must start with start_object");
            }
        }
        if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
            // we encountered '{}' for a query clause, it used to be supported, deprecated in 5.0 and removed in 6.0
            throw new IllegalArgumentException("query malformed, empty clause found at [" + parser.getTokenLocation() +"]");
        }
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, no field after start_object");
        }
        String queryName = parser.currentName();
        // move to the next START_OBJECT
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + queryName + "] query malformed, no start_object after query name");
        }
        QueryBuilder result;
        try {
            result = parser.namedObject(QueryBuilder.class, queryName, null);
        } catch (UnknownNamedObjectException e) {
            // Preserve the error message from 5.0 until we have a compellingly better message so we don't break BWC.
            // This intentionally doesn't include the causing exception because that'd change the "root_cause" of any unknown query errors
            throw new ParsingException(new XContentLocation(e.getLineNumber(), e.getColumnNumber()),
                    "no [query] registered for [" + e.getName() + "]");
        }
        //end_object of the specific query (e.g. match, multi_match etc.) element
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                    "[" + queryName + "] malformed query, expected [END_OBJECT] but found [" + parser.currentToken() + "]");
        }
        //end_object of the query object
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                    "[" + queryName + "] malformed query, expected [END_OBJECT] but found [" + parser.currentToken() + "]");
        }
        return result;
    }

    // Like Objects.requireNotNull(...) but instead throws a IllegalArgumentException
    protected static <T> T requireValue(T value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    protected static void throwParsingExceptionOnMultipleFields(String queryName, XContentLocation contentLocation,
                                                                String processedFieldName, String currentFieldName) {
        if (processedFieldName != null) {
            throw new ParsingException(contentLocation, "[" + queryName + "] query doesn't support multiple fields, found ["
                    + processedFieldName + "] and [" + currentFieldName + "]");
        }
    }

    /**
     * Adds {@code boost} and {@code query_name} parsing to the
     * {@link AbstractObjectParser} passed in. All query builders except
     * {@link MatchAllQueryBuilder} and {@link MatchNoneQueryBuilder} support these fields so they
     * should use this method.
     */
    protected static void declareStandardFields(AbstractObjectParser<? extends QueryBuilder, ?> parser) {
        parser.declareFloat(QueryBuilder::boost, AbstractQueryBuilder.BOOST_FIELD);
        parser.declareString(QueryBuilder::queryName, AbstractQueryBuilder.NAME_FIELD);
    }

    @Override
    public final String toString() {
        return Strings.toString(this, true, true);
    }
}
