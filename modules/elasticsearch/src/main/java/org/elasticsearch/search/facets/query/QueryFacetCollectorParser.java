/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.facets.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.xcontent.XContentIndexQueryParser;
import org.elasticsearch.search.facets.collector.FacetCollector;
import org.elasticsearch.search.facets.collector.FacetCollectorParser;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.util.xcontent.XContentParser;

/**
 * @author kimchy (Shay Banon)
 */
public class QueryFacetCollectorParser implements FacetCollectorParser {

    public static final String NAME = "query";

    @Override public String name() {
        return "query";
    }

    @Override public FacetCollector parser(String facetName, XContentParser parser, SearchContext context) {
        XContentIndexQueryParser indexQueryParser = (XContentIndexQueryParser) context.queryParser();
        Query facetQuery = indexQueryParser.parse(parser);
        return new QueryFacetCollector(facetName, facetQuery, context.filterCache());
    }
}
