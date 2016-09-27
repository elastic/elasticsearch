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
import java.util.List;
import java.util.Map;

import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper.GeoShapeFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.locationtech.spatial4j.shape.Shape;

/**
 */
public class GeoHeatmapAggregatorFactory extends AggregatorFactory<GeoHeatmapAggregatorFactory> {

    private final int maxCells;
    private final int gridLevel;
    private final Shape inputShape;
    private final PrefixTreeStrategy strategy;

    /**
     * Called by the {@link GeoHeatmapAggregationBuilder}
     * 
     * @param name
     *            the name of this heatmap aggregator
     * @param type
     *            passed in from the Builder
     * @param field
     *            the indexed field on which to create the heatmap; must be a
     *            geo_shape
     * @param inputShape
     *            indexed shapes must intersect inputShape to be counted; if
     *            null the world rectangle is used
     * @param maxCells
     *            the maximum number of cells (grid squares) that could possibly
     *            be returned from the heatmap
     * @param gridLevel
     *            manually set the granularity of the grid
     * @param context
     *            the context of this aggregation
     * @param parent
     *            the parent aggregation
     * @param subFactoriesBuilder
     *            (not used)
     * @param metaData
     *            aggregation metadata
     * @throws IOException
     *             if an error occurs creating the factory
     */
    public GeoHeatmapAggregatorFactory(String name, Type type, String field, Shape inputShape, int maxCells, int gridLevel,
            AggregationContext context, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder,
            Map<String, Object> metaData) throws IOException {

        super(name, type, context, parent, subFactoriesBuilder, metaData);
        MappedFieldType fieldType = context.searchContext().mapperService().fullName(field);
        if (fieldType.typeName().equals(GeoShapeFieldMapper.CONTENT_TYPE)) {
            GeoShapeFieldType geoFieldType = (GeoShapeFieldType) fieldType;
            this.strategy = geoFieldType.resolveStrategy(SpatialStrategy.RECURSIVE);

        } else {
            throw new AggregationInitializationException(
                    "Field [" + field + "] is a " + fieldType.typeName() + " instead of a " + GeoShapeFieldMapper.CONTENT_TYPE + " type");
        }
        this.inputShape = inputShape;
        this.maxCells = maxCells;
        this.gridLevel = gridLevel;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        return new GeoHeatmapAggregator(name, inputShape, strategy, maxCells, gridLevel, context, parent, pipelineAggregators, metaData);

    }

}
