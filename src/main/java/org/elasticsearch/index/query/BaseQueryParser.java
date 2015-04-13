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

import java.io.IOException;

public abstract class BaseQueryParser implements QueryParser {

    public abstract String[] names();

    public abstract Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException;

    /**
     * this method impl is an intermediate step in the refactoring that should be overwritten
     * by all queries that already implement the toQuery/fromXContent split correctly
     * To be removed once all queries are refactored
     */
    public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
            Query query = parse(parseContext);
            return new QueryWrappingQueryBuilder(query);
    }
}
