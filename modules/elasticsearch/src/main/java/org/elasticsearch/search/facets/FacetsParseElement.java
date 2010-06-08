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

package org.elasticsearch.search.facets;

import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.facets.collector.FacetCollector;
import org.elasticsearch.search.facets.collector.FacetCollectorParser;
import org.elasticsearch.search.facets.histogram.HistogramFacetCollectorParser;
import org.elasticsearch.search.facets.query.QueryFacetCollectorParser;
import org.elasticsearch.search.facets.statistical.StatisticalFacetCollectorParser;
import org.elasticsearch.search.facets.terms.TermsFacetCollectorParser;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.util.MapBuilder;
import org.elasticsearch.util.collect.ImmutableMap;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.xcontent.XContentParser;

import java.util.List;

import static org.elasticsearch.util.MapBuilder.*;

/**
 * <pre>
 * facets : {
 *  facet1: {
 *      query : { ... },
 *      global : false
 *  },
 *  facet2: {
 *      terms : {
 *          name : "myfield",
 *          size : 12
 *      },
 *      global : false
 *  }
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class FacetsParseElement implements SearchParseElement {

    private final ImmutableMap<String, FacetCollectorParser> facetCollectorParsers;

    public FacetsParseElement() {
        MapBuilder<String, FacetCollectorParser> builder = newMapBuilder();
        builder.put(TermsFacetCollectorParser.NAME, new TermsFacetCollectorParser());
        builder.put(QueryFacetCollectorParser.NAME, new QueryFacetCollectorParser());
        builder.put(StatisticalFacetCollectorParser.NAME, new StatisticalFacetCollectorParser());
        builder.put(HistogramFacetCollectorParser.NAME, new HistogramFacetCollectorParser());
        this.facetCollectorParsers = builder.immutableMap();
    }

    @Override public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;

        List<FacetCollector> facetCollectors = null;

        String topLevelFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                topLevelFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                FacetCollector facet = null;
                boolean global = false;
                String facetFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        facetFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        FacetCollectorParser facetCollectorParser = facetCollectorParsers.get(facetFieldName);
                        if (facetCollectorParser == null) {
                            throw new SearchParseException(context, "No facet type for [" + facetFieldName + "]");
                        }
                        facet = facetCollectorParser.parser(topLevelFieldName, parser, context);
                    } else if (token.isValue()) {
                        if ("global".equals(facetFieldName)) {
                            global = parser.booleanValue();
                        }
                    }
                }

                if (facetCollectors == null) {
                    facetCollectors = Lists.newArrayList();
                }
                facetCollectors.add(facet);
                if (global) {
                    context.searcher().addGlobalCollector(facet);
                } else {
                    context.searcher().addCollector(facet);
                }
            }
        }

        context.facets(new SearchContextFacets(facetCollectors));
    }
}
