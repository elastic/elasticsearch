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

package org.elasticsearch.search.facet;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacetBuilder;
import org.elasticsearch.search.facet.filter.FilterFacetBuilder;
import org.elasticsearch.search.facet.geodistance.GeoDistanceFacetBuilder;
import org.elasticsearch.search.facet.histogram.HistogramFacetBuilder;
import org.elasticsearch.search.facet.histogram.HistogramScriptFacetBuilder;
import org.elasticsearch.search.facet.query.QueryFacetBuilder;
import org.elasticsearch.search.facet.range.RangeFacetBuilder;
import org.elasticsearch.search.facet.range.RangeScriptFacetBuilder;
import org.elasticsearch.search.facet.statistical.StatisticalFacetBuilder;
import org.elasticsearch.search.facet.statistical.StatisticalScriptFacetBuilder;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacetBuilder;

/**
 * @deprecated Facets are deprecated and will be removed in a future release. Please use aggregations instead.
 */
@Deprecated
public class FacetBuilders {

    public static QueryFacetBuilder queryFacet(String facetName) {
        return new QueryFacetBuilder(facetName);
    }

    public static QueryFacetBuilder queryFacet(String facetName, QueryBuilder query) {
        return new QueryFacetBuilder(facetName).query(query);
    }

    public static FilterFacetBuilder filterFacet(String facetName) {
        return new FilterFacetBuilder(facetName);
    }

    public static FilterFacetBuilder filterFacet(String facetName, FilterBuilder filter) {
        return new FilterFacetBuilder(facetName).filter(filter);
    }

    public static TermsFacetBuilder termsFacet(String facetName) {
        return new TermsFacetBuilder(facetName);
    }

    public static TermsStatsFacetBuilder termsStatsFacet(String facetName) {
        return new TermsStatsFacetBuilder(facetName);
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

    public static DateHistogramFacetBuilder dateHistogramFacet(String facetName) {
        return new DateHistogramFacetBuilder(facetName);
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
