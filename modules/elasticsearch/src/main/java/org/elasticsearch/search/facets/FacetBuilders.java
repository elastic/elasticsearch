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

import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
import org.elasticsearch.search.facets.geodistance.GeoDistanceFacetBuilder;
import org.elasticsearch.search.facets.histogram.HistogramFacetBuilder;
import org.elasticsearch.search.facets.histogram.HistogramScriptFacetBuilder;
import org.elasticsearch.search.facets.query.QueryFacetBuilder;
import org.elasticsearch.search.facets.range.RangeFacetBuilder;
import org.elasticsearch.search.facets.range.RangeScriptFacetBuilder;
import org.elasticsearch.search.facets.statistical.StatisticalFacetBuilder;
import org.elasticsearch.search.facets.statistical.StatisticalScriptFacetBuilder;
import org.elasticsearch.search.facets.terms.TermsFacetBuilder;

/**
 * @author kimchy (shay.banon)
 */
public class FacetBuilders {

    public static QueryFacetBuilder queryFacet(String facetName) {
        return new QueryFacetBuilder(facetName);
    }

    public static QueryFacetBuilder queryFacet(String facetName, XContentQueryBuilder query) {
        return new QueryFacetBuilder(facetName).query(query);
    }

    public static TermsFacetBuilder termsFacet(String facetName) {
        return new TermsFacetBuilder(facetName);
    }

    public static StatisticalFacetBuilder statisticalFacet(String facetName) {
        return new StatisticalFacetBuilder(facetName);
    }

    public static StatisticalScriptFacetBuilder statisticalScriptFacet(String facetName) {
        return new StatisticalScriptFacetBuilder(facetName);
    }

    public static HistogramFacetBuilder histogramFacet(String facetName) {
        return new HistogramFacetBuilder(facetName);
    }

    public static HistogramScriptFacetBuilder histogramScriptFacet(String facetName) {
        return new HistogramScriptFacetBuilder(facetName);
    }

    public static RangeFacetBuilder rangeFacet(String facetName) {
        return new RangeFacetBuilder(facetName);
    }

    public static RangeScriptFacetBuilder rangeScriptFacet(String facetName) {
        return new RangeScriptFacetBuilder(facetName);
    }

    public static GeoDistanceFacetBuilder geoDistanceFacet(String facetName) {
        return new GeoDistanceFacetBuilder(facetName);
    }
}
