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

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.Nullable;

import java.io.IOException;

/**
 *
 */
public interface FilterParser {

    /**
     * The names this filter is registered under.
     */
    String[] names();

    /**
     * Parses the into a filter from the current parser location. Will be at "START_OBJECT" location,
     * and should end when the token is at the matching "END_OBJECT".
     * <p/>
     * The parser should return null value when it should be ignored, regardless under which context
     * it is. For example, an and filter with "and []" (no clauses), should be ignored regardless if
     * it exists within a must clause or a must_not bool clause (that is why returning MATCH_ALL will
     * not be good, since it will not match anything when returned within a must_not clause).
     */
    @Nullable
    Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException;
}