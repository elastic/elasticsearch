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
import org.elasticsearch.common.Nullable;

import java.io.IOException;

/**
 *
 */
public interface QueryParser {

    /**
     * The names this query parser is registered under.
     */
    String[] names();

    /**
     * Parses the into a query from the current parser location. Will be at
     * "START_OBJECT" location, and should end when the token is at the matching
     * "END_OBJECT".
     * <p/>
     * Returns <tt>null</tt> if this query should be ignored in the context of
     * the DSL.
     */
    //norelease can be removed in favour of fromXContent once search requests can be parsed on the coordinating node
    @Nullable
    Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException;

    /**
     * Creates a new {@link QueryBuilder} from the query held by the {@link QueryParseContext}
     * in {@link org.elasticsearch.common.xcontent.XContent} format
     *
     * @param parseContext
     *            the input parse context. The state on the parser contained in
     *            this context will be changed as a side effect of this method
     *            call
     * @return the new QueryBuilder
     * @throws IOException
     * @throws QueryParsingException
     */
    QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException;

    /**
     * @return an empty {@link QueryBuilder} instance for this parser that can be used for deserialization
     */
    QueryBuilder getBuilderPrototype();
}
