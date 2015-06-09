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
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * Base class for all classes producing lucene queries.
 * Supports conversion to BytesReference and creation of lucene Query objects.
 */
public abstract class QueryBuilder<QB extends QueryBuilder> extends ToXContentToBytes implements NamedWriteable<QB> {

    protected QueryBuilder() {
        super(XContentType.JSON);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContent(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * @return a unique name this query is identified with
     */
    public abstract String queryId();

    @Override
    public final String getName() {
        return queryId();
    }

    /**
     * Converts this QueryBuilder to a lucene {@link Query}
     * @param parseContext additional information needed to construct the queries
     * @return the {@link Query}
     * @throws QueryParsingException
     * @throws IOException
     */
    //norelease to be made abstract once all query builders override toQuery providing their own specific implementation.
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        return parseContext.indexQueryParserService().queryParser(queryId()).parse(parseContext);
    }

    /**
     * Validate the query.
     * @return a {@link QueryValidationException} containing error messages, {@code null} if query is valid.
     * e.g. if fields that are needed to create the lucene query are missing.
     */
    public QueryValidationException validate() {
        // default impl does not validate, subclasses should override.
        //norelease to be possibly made abstract once all queries support validation
        return null;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

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

    //norelease remove this once all builders implement readFrom themselves
    @Override
    public QB readFrom(StreamInput in) throws IOException {
        return null;
    }

    //norelease remove this once all builders implement writeTo themselves
    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }
}
