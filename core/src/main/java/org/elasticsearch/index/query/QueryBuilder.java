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
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public interface QueryBuilder<QB extends QueryBuilder> extends NamedWriteable<QB>, ToXContent {
    
    /**
     * Validate the query.
     * @return a {@link QueryValidationException} containing error messages, {@code null} if query is valid.
     * e.g. if fields that are needed to create the lucene query are missing.
     */
    QueryValidationException validate();

    /**
     * Converts this QueryBuilder to a lucene {@link Query}.
     * Returns <tt>null</tt> if this query should be ignored in the context of
     * parent queries.
     *
     * @param parseContext additional information needed to construct the queries
     * @return the {@link Query} or <tt>null</tt> if this query should be ignored upstream
     * @throws QueryParsingException
     * @throws IOException
     */
    Query toQuery(QueryParseContext parseContext) throws IOException;

    /**
     * Returns a {@link org.elasticsearch.common.bytes.BytesReference}
     * containing the {@link ToXContent} output in binary format.
     * Builds the request based on the default {@link XContentType}, either {@link Requests#CONTENT_TYPE} or provided as a constructor argument
     */
    //norelease once we move to serializing queries over the wire in Streamable format, this method shouldn't be needed anymore
    BytesReference buildAsBytes();
}
