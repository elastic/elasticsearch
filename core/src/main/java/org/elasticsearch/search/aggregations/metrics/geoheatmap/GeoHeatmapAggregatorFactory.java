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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.unit.DistanceUnit;
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

    public static final double DEFAULT_DIST_ERR_PCT = 0.15;
    public static final int DEFAULT_MAX_CELLS = 100_000;

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
     * @param distErr
     *            the maximum error distance allowable between the indexed shape and the cells 
     * @param distErrPct
     *            the maximum error ratio between the indexed shape and the heatmap shape
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
    public GeoHeatmapAggregatorFactory(String name, Type type, String field, Optional<Shape> inputShape, Optional<Integer> maxCells, 
            Optional<Double> distErr, Optional<Double> distErrPct, Optional<Integer> gridLevel,
            AggregationContext context, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder,
            Map<String, Object> metaData) throws IOException {

        super(name, type, context, parent, subFactoriesBuilder, metaData);
        MappedFieldType fieldType = context.searchContext().mapperService().fullName(field);
        if (fieldType.typeName().equals(GeoShapeFieldMapper.CONTENT_TYPE)) {
            GeoShapeFieldType geoFieldType = (GeoShapeFieldType) fieldType;
            this.strategy = geoFieldType.resolveStrategy(SpatialStrategy.RECURSIVE);
        } else {
            throw new AggregationInitializationException(String.format(Locale.ROOT,
                    "Field [%s] is a %s instead of a %s type",
                    field, fieldType.typeName(), GeoShapeFieldMapper.CONTENT_TYPE));
        }
        this.inputShape = inputShape.orElse(strategy.getSpatialContext().getWorldBounds());
        this.maxCells = maxCells.orElse(DEFAULT_MAX_CELLS);
        this.gridLevel = gridLevel.orElse(resolveGridLevel(distErr, distErrPct));
    }

    @Override
    public Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        return new GeoHeatmapAggregator(name, inputShape, strategy, maxCells, gridLevel, context, parent, pipelineAggregators, metaData);
    }
    
    private Integer resolveGridLevel(Optional<Double> distErrOp, Optional<Double> distErrPctOp) {
        SpatialArgs spatialArgs = new SpatialArgs(SpatialOperation.Intersects, this.inputShape);
        if (distErrOp.isPresent()) {
            spatialArgs.setDistErr(distErrOp.get() * DistanceUnit.DEFAULT.getDistancePerDegree());
        }    
        spatialArgs.setDistErrPct(distErrPctOp.orElse(DEFAULT_DIST_ERR_PCT));
        double distErr = spatialArgs.resolveDistErr(strategy.getSpatialContext(), DEFAULT_DIST_ERR_PCT);
        if (distErr <= 0) {
          throw new AggregationInitializationException(String.format(Locale.ROOT,
              "%s or %s should be > 0 or instead provide %s=%s for absolute maximum detail", 
                  GeoHeatmapAggregationBuilder.DIST_ERR_PCT_FIELD, 
                  GeoHeatmapAggregationBuilder.DIST_ERR_FIELD,
                  GeoHeatmapAggregationBuilder.GRID_LEVEL_FIELD,
                  strategy.getGrid().getMaxLevels()));
        }
        return strategy.getGrid().getLevelForDistance(distErr);
    }

}
