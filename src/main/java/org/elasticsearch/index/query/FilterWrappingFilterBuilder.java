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
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * FilterBuilder implementation that  holds a lucene filter, which can be returned by {@link #toFilter(QueryParseContext)}.
 * Doesn't support conversion to {@link org.elasticsearch.common.xcontent.XContent} via {@link #doXContent(XContentBuilder, Params)}.
 */
//norelease to be removed once all queries support separate fromXContent and toQuery methods
public class FilterWrappingFilterBuilder extends BaseFilterBuilder {

    private final Filter filter;

    public FilterWrappingFilterBuilder(Filter filter) {
        this.filter = filter;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Filter toFilter(QueryParseContext parseContext) throws IOException {
        return this.filter;
    }

    @Override
    protected String parserName() {
        // this should not be called since we overwrite BaseFilterBuilder#toFilter() in this class
        throw new UnsupportedOperationException();
    }
}
