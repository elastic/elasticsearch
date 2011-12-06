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

package org.elasticsearch.search.facet;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.search.facet.datehistogram.InternalDateHistogramFacet;
import org.elasticsearch.search.facet.filter.InternalFilterFacet;
import org.elasticsearch.search.facet.geodistance.InternalGeoDistanceFacet;
import org.elasticsearch.search.facet.histogram.InternalHistogramFacet;
import org.elasticsearch.search.facet.query.InternalQueryFacet;
import org.elasticsearch.search.facet.range.InternalRangeFacet;
import org.elasticsearch.search.facet.statistical.InternalStatisticalFacet;
import org.elasticsearch.search.facet.terms.InternalTermsFacet;
import org.elasticsearch.search.facet.termsstats.InternalTermsStatsFacet;

/**
 *
 */
public class TransportFacetModule extends AbstractModule {

    @Override
    protected void configure() {
        InternalFilterFacet.registerStreams();
        InternalQueryFacet.registerStreams();
        InternalGeoDistanceFacet.registerStreams();
        InternalHistogramFacet.registerStreams();
        InternalDateHistogramFacet.registerStreams();
        InternalRangeFacet.registerStreams();
        InternalStatisticalFacet.registerStreams();
        InternalTermsFacet.registerStreams();
        InternalTermsStatsFacet.registerStreams();
    }
}
