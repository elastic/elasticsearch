/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet.filter;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class FilterFacetParser extends AbstractComponent implements FacetParser {

    @Inject
    public FilterFacetParser(Settings settings) {
        super(settings);
        InternalFilterFacet.registerStreams();
    }

    @Override
    public String[] types() {
        return new String[]{FilterFacet.TYPE};
    }

    @Override
    public FacetExecutor.Mode defaultMainMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor.Mode defaultGlobalMode() {
        return FacetExecutor.Mode.POST;
    }

    @Override
    public FacetExecutor parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        ParsedFilter parsedFilter = context.queryParserService().parseInnerFilter(parser);
        return new FilterFacetExecutor(parsedFilter == null ? Queries.MATCH_ALL_FILTER : parsedFilter.filter());
    }
}
