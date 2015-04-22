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
package org.elasticsearch.search.aggregations;

import com.google.common.collect.ImmutableList;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.search.aggregations.bucket.children.InternalChildren;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filters.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissing;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.date.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.InternalGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.InternalIPv4Range;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.UnmappedSampler;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.significant.UnmappedSignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.TransportSignificantTermsHeuristicModule;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.InternalGeoBounds;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.InternalScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;

/**
 * A module that registers all the transport streams for the addAggregation
 */
public class TransportAggregationModule extends AbstractModule implements SpawnModules {

    @Override
    protected void configure() {

        // calcs
        InternalAvg.registerStreams();
        InternalSum.registerStreams();
        InternalMin.registerStreams();
        InternalMax.registerStreams();
        InternalStats.registerStreams();
        InternalExtendedStats.registerStreams();
        InternalValueCount.registerStreams();
        InternalPercentiles.registerStreams();
        InternalPercentileRanks.registerStreams();
        InternalCardinality.registerStreams();
        InternalScriptedMetric.registerStreams();

        // buckets
        InternalGlobal.registerStreams();
        InternalFilter.registerStreams();
        InternalFilters.registerStream();
        InternalSampler.registerStreams();
        UnmappedSampler.registerStreams();
        InternalMissing.registerStreams();
        StringTerms.registerStreams();
        LongTerms.registerStreams();
        SignificantStringTerms.registerStreams();
        SignificantLongTerms.registerStreams();
        UnmappedSignificantTerms.registerStreams();
        InternalGeoHashGrid.registerStreams();                
        DoubleTerms.registerStreams();
        UnmappedTerms.registerStreams();
        InternalRange.registerStream();
        InternalDateRange.registerStream();
        InternalIPv4Range.registerStream();
        InternalHistogram.registerStream();
        InternalGeoDistance.registerStream();
        InternalNested.registerStream();
        InternalReverseNested.registerStream();
        InternalTopHits.registerStreams();
        InternalGeoBounds.registerStream();
        InternalChildren.registerStream();
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new TransportSignificantTermsHeuristicModule());
    }
}
