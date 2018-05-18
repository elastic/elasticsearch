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

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder.CellIdSource;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.GeoPoint;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GeoHashGridAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint, GeoHashGridAggregatorFactory> {

    private final int precision;
    private final int requiredSize;
    private final int shardSize;

    GeoHashGridAggregatorFactory(String name, ValuesSourceConfig<GeoPoint> config, int precision, int requiredSize,
            int shardSize, SearchContext context, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder,
            Map<String, Object> metaData) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.precision = precision;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        final InternalAggregation aggregation = new InternalGeoHashGrid(name, requiredSize,
                Collections.<InternalGeoHashGrid.Bucket> emptyList(), pipelineAggregators, metaData);
        return new NonCollectingAggregator(name, context, parent, pipelineAggregators, metaData) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(final ValuesSource.GeoPoint valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }
        CellIdSource cellIdSource = new CellIdSource(valuesSource, precision);
        return new GeoHashGridAggregator(name, factories, cellIdSource, requiredSize, shardSize, context, parent,
                pipelineAggregators, metaData);

    }

}
