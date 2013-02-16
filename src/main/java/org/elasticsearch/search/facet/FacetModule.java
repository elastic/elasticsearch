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

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacetParser;
import org.elasticsearch.search.facet.filter.FilterFacetParser;
import org.elasticsearch.search.facet.geodistance.GeoDistanceFacetParser;
import org.elasticsearch.search.facet.histogram.HistogramFacetParser;
import org.elasticsearch.search.facet.query.QueryFacetParser;
import org.elasticsearch.search.facet.range.RangeFacetParser;
import org.elasticsearch.search.facet.statistical.StatisticalFacetParser;
import org.elasticsearch.search.facet.terms.TermsFacetParser;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacetParser;

import java.util.List;

/**
 *
 */
public class FacetModule extends AbstractModule {

    private List<Class<? extends FacetParser>> processors = Lists.newArrayList();

    public FacetModule() {
        processors.add(FilterFacetParser.class);
        processors.add(QueryFacetParser.class);
        processors.add(GeoDistanceFacetParser.class);
        processors.add(HistogramFacetParser.class);
        processors.add(DateHistogramFacetParser.class);
        processors.add(RangeFacetParser.class);
        processors.add(StatisticalFacetParser.class);
        processors.add(TermsFacetParser.class);
        processors.add(TermsStatsFacetParser.class);
    }

    public void addFacetProcessor(Class<? extends FacetParser> facetProcessor) {
        processors.add(facetProcessor);
    }

    @Override
    protected void configure() {
        Multibinder<FacetParser> multibinder = Multibinder.newSetBinder(binder(), FacetParser.class);
        for (Class<? extends FacetParser> processor : processors) {
            multibinder.addBinding().to(processor);
        }
        bind(FacetParsers.class).asEagerSingleton();
        bind(FacetParseElement.class).asEagerSingleton();
        bind(FacetPhase.class).asEagerSingleton();
    }
}
