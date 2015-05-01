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

import java.io.IOException;

/**
 * This class with method impl is an intermediate step in the filter parsers refactoring.
 * Provides a fromXContent default implementation for filter parsers that don't have yet a
 * specific fromXContent implementation that returns a FilterBuilder.
 * To be removed once all filters are moved over to extend {@link BaseFilterParser}.
 */
public abstract class BaseFilterParserTemp implements FilterParser {

    @Override
    public FilterBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        Filter filter = parse(parseContext);
        return new FilterWrappingFilterBuilder(filter);
    }
}
