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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Base class for all classes producing lucene queries.
 * Supports conversion to BytesReference and creation of lucene Query objects.
 */
public abstract class AbstractQueryBuilder<QB extends AbstractQueryBuilder> extends ToXContentToBytes implements QueryBuilder<QB> {

    /** Default for boost to apply to resulting Lucene query. Defaults to 1.0*/
    public static final float DEFAULT_BOOST = 1.0f;

    protected String queryName;
    protected float boost = DEFAULT_BOOST;

    protected AbstractQueryBuilder() {
        super(XContentType.JSON);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    protected void printBoostAndQueryName(XContentBuilder builder) throws IOException {
        builder.field("boost", boost);
        if (queryName != null) {
            builder.field("_name", queryName);
        }
    }

    @Override
    public final Query toQuery(QueryParseContext parseContext) throws IOException {
        Query query = doToQuery(parseContext);
        if (query != null) {
            query.setBoost(boost);
            if (queryName != null) {
                parseContext.addNamedQuery(queryName, query);
            }
        }
        return query;
    }

    //norelease to be made abstract once all query builders override doToQuery providing their own specific implementation.
    protected Query doToQuery(QueryParseContext parseContext) throws IOException {
        return parseContext.indexQueryParserService().queryParser(getName()).parse(parseContext);
    }

    @Override
    public QueryValidationException validate() {
        // default impl does not validate, subclasses should override.
        //norelease to be possibly made abstract once all queries support validation
        return null;
    }

    /**
     * Returns the query name for the query.
     */
    @SuppressWarnings("unchecked")
    public QB queryName(String queryName) {
        this.queryName = queryName;
        return (QB) this;
    }

    /**
     * Sets the query name for the query.
     */
    public final String queryName() {
        return queryName;
    }

    /**
     * Returns the boost for this query.
     */
    public final float boost() {
        return this.boost;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @SuppressWarnings("unchecked")
    public QB boost(float boost) {
        this.boost = boost;
        return (QB) this;
    }

    @Override
    public final QB readFrom(StreamInput in) throws IOException {
        QB queryBuilder = doReadFrom(in);
        queryBuilder.boost = in.readFloat();
        queryBuilder.queryName = in.readOptionalString();
        return queryBuilder;
    }

    //norelease make this abstract once all builders implement doReadFrom themselves
    protected QB doReadFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        doWriteTo(out);
        out.writeFloat(boost);
        out.writeOptionalString(queryName);
    }

    //norelease make this abstract once all builders implement doWriteTo themselves
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
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
    //norelease to be made abstract once all queries are refactored
    protected boolean doEquals(QB other) {
        return super.equals(other);
    }

    @Override
    public final int hashCode() {
        return 31 * Objects.hash(getClass(), queryName, boost) + doHashCode();
    }

    //norelease to be made abstract once all queries are refactored
    protected int doHashCode() {
        return super.hashCode();
    }

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
     * their {@link QueryBuilder#toQuery(QueryParseContext)} method are not added to the
     * resulting collection.
     *
     * @throws IOException
     * @throws QueryParsingException
     */
    protected static Collection<Query> toQueries(Collection<QueryBuilder> queryBuilders, QueryParseContext parseContext) throws QueryParsingException,
            IOException {
        List<Query> queries = new ArrayList<>(queryBuilders.size());
        for (QueryBuilder queryBuilder : queryBuilders) {
            Query query = queryBuilder.toQuery(parseContext);
            if (query != null) {
                queries.add(query);
            }
        }
        return queries;
    }

    /**
     * Utility method that converts inner query builders to xContent and
     * checks for null values, rendering out empty object in this case.
     */
    protected static void doXContentInnerBuilder(XContentBuilder xContentBuilder, String fieldName,
            QueryBuilder queryBuilder, Params params) throws IOException {
        xContentBuilder.field(fieldName);
        if (queryBuilder != null) {
            queryBuilder.toXContent(xContentBuilder, params);
        } else {
            // we output an empty object, QueryParseContext#parseInnerQueryBuilder will parse this back to `null` value
            xContentBuilder.startObject();
            xContentBuilder.endObject();
        }
    }

    protected static QueryValidationException validateInnerQueries(List<QueryBuilder> queryBuilders, QueryValidationException initialValidationException) {
        QueryValidationException validationException = initialValidationException;
        for (QueryBuilder queryBuilder : queryBuilders) {
            validationException = validateInnerQuery(queryBuilder, validationException);
        }
        return validationException;
    }

    protected static QueryValidationException validateInnerQuery(QueryBuilder queryBuilder, QueryValidationException initialValidationException) {
        QueryValidationException validationException = initialValidationException;
        if (queryBuilder != null) {
            QueryValidationException queryValidationException = queryBuilder.validate();
            if (queryValidationException != null) {
                validationException = QueryValidationException.addValidationErrors(queryValidationException.validationErrors(), validationException);
            }
        }
        return validationException;
    }
}
