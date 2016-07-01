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

import java.io.IOException;
import java.util.Optional;

/**
 * Defines a query parser that is able to parse {@link QueryBuilder}s from {@link org.elasticsearch.common.xcontent.XContent}.
 */
@FunctionalInterface
public interface QueryParser<QB extends QueryBuilder> {
    /**
     * Creates a new {@link QueryBuilder} from the query held by the {@link QueryParseContext}
     * in {@link org.elasticsearch.common.xcontent.XContent} format
     *
     * @param parseContext
     *            the input parse context. The state on the parser contained in
     *            this context will be changed as a side effect of this method
     *            call
     * @return the new QueryBuilder
     */
    Optional<QB> fromXContent(QueryParseContext parseContext) throws IOException;
}
