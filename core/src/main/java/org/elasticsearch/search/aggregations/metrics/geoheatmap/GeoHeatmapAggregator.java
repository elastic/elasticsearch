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

package org.elasticsearch.search.aggregations.metrics.geoheatmap;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.spatial.prefix.HeatmapFacetCounter;
import org.apache.lucene.spatial.prefix.HeatmapFacetCounter.Heatmap;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Aggregates hits on geo_shape fields as counts on a grid
 */
public class GeoHeatmapAggregator extends MetricsAggregator {

    private final int maxCells;
    private final int gridLevel;
    private final Shape inputShape;
    private final PrefixTreeStrategy strategy;
    private IndexReaderContext parentReaderContext;
    private Map<Long, SparseFixedBitSet> buckets = new HashMap<>();

    /**
     * Used by the {@link GeoHeatmapAggregatorFactory} to create a new instance
     * 
     * @param name
     *            the name of this heatmap aggregator
     * @param inputShape
     *            indexed shapes must intersect inputShape to be counted; if
     *            null the world rectangle is used
     * @param strategy
     *            a geo strategy for searching the field
     * @param maxCells
     *            the maximum number of cells (grid squares) that could possibly
     *            be returned from the heatmap
     * @param gridLevel
     *            manually set the granularity of the grid
     * @param aggregationContext
     *            the context of this aggregation
     * @param parent
     *            the parent aggregation
     * @param pipelineAggregators
     *            any pipeline aggregations attached
     * @param metaData
     *            aggregation metadata
     * @throws IOException
     *             when parsing fails
     */
    public GeoHeatmapAggregator(String name, Shape inputShape, PrefixTreeStrategy strategy, int maxCells, int gridLevel,
            AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        super(name, aggregationContext, parent, pipelineAggregators, metaData);
        this.inputShape = inputShape;
        this.strategy = strategy;
        this.maxCells = maxCells;
        this.gridLevel = gridLevel;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (parentReaderContext == null) {
            parentReaderContext = ctx.parent;
        } else {
            assert ctx.parent == parentReaderContext;
        }
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                SparseFixedBitSet bits = buckets.get(bucket);
                if (bits == null) {
                    bits = new SparseFixedBitSet(parentReaderContext.reader().maxDoc());
                    buckets.put(bucket, bits);
                }
                bits.set(ctx.docBase + doc);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        SparseFixedBitSet matchingDocs = buckets.get(owningBucketOrdinal);
        if (matchingDocs == null)
            return buildEmptyAggregation();

        Heatmap heatmap = HeatmapFacetCounter.calcFacets(strategy, parentReaderContext, matchingDocs, inputShape, gridLevel, maxCells);

        return new InternalGeoHeatmap(name, gridLevel, heatmap.rows, heatmap.columns, heatmap.region.getMinX(), heatmap.region.getMinY(),
                heatmap.region.getMaxX(), heatmap.region.getMaxY(), heatmap.counts, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoHeatmap(name, gridLevel, 0, 0, 0, 0, 0, 0, new int[0], pipelineAggregators(), metaData());
    }

}
