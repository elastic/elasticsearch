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

/**
 * Base class for all classes producing lucene queries.
 * Supports conversion to BytesReference and creation of lucene Query objects.
 */
public abstract class AbstractQueryBuilder<QB extends QueryBuilder> extends ToXContentToBytes implements QueryBuilder<QB> {

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

    @Override
    //norelease to be made abstract once all query builders override toQuery providing their own specific implementation.
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        return parseContext.indexQueryParserService().queryParser(getName()).parse(parseContext);
    }

    @Override
    public QueryValidationException validate() {
        // default impl does not validate, subclasses should override.
        //norelease to be possibly made abstract once all queries support validation
        return null;
    }

    //norelease remove this once all builders implement readFrom themselves
    @Override
    public QB readFrom(StreamInput in) throws IOException {
        return null;
    }

    //norelease remove this once all builders implement writeTo themselves
    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        return doEquals(other);
    }

    /**
     * Indicates whether some other {@link QueryBuilder} object of the same type is "equal to" this one.
     */
    //norelease to be made abstract once all queries are refactored
    protected boolean doEquals(QB other) {
        throw new UnsupportedOperationException();
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
}
