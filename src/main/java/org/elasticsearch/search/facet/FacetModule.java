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
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacetProcessor;
import org.elasticsearch.search.facet.filter.FilterFacetProcessor;
import org.elasticsearch.search.facet.geodistance.GeoDistanceFacetProcessor;
import org.elasticsearch.search.facet.histogram.HistogramFacetProcessor;
import org.elasticsearch.search.facet.query.QueryFacetProcessor;
import org.elasticsearch.search.facet.range.RangeFacetProcessor;
import org.elasticsearch.search.facet.statistical.StatisticalFacetProcessor;
import org.elasticsearch.search.facet.terms.TermsFacetProcessor;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacetProcessor;

import java.util.List;

/**
 *
 */
public class FacetModule extends AbstractModule {

    private List<Class<? extends FacetProcessor>> processors = Lists.newArrayList();

    public FacetModule() {
        processors.add(FilterFacetProcessor.class);
        processors.add(QueryFacetProcessor.class);
        processors.add(GeoDistanceFacetProcessor.class);
        processors.add(HistogramFacetProcessor.class);
        processors.add(DateHistogramFacetProcessor.class);
        processors.add(RangeFacetProcessor.class);
        processors.add(StatisticalFacetProcessor.class);
        processors.add(TermsFacetProcessor.class);
        processors.add(TermsStatsFacetProcessor.class);
    }

    public void addFacetProcessor(Class<? extends FacetProcessor> facetProcessor) {
        processors.add(facetProcessor);
    }

    @Override
    protected void configure() {
        Multibinder<FacetProcessor> multibinder = Multibinder.newSetBinder(binder(), FacetProcessor.class);
        for (Class<? extends FacetProcessor> processor : processors) {
            multibinder.addBinding().to(processor);
        }
        bind(FacetProcessors.class).asEagerSingleton();
        bind(FacetParseElement.class).asEagerSingleton();
        bind(FacetPhase.class).asEagerSingleton();
    }
}
