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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Collects the various parameters for a heatmap aggregation and builds a
 * factory
 */
public class GeoHeatmapAggregationBuilder extends AbstractAggregationBuilder<GeoHeatmapAggregationBuilder> {
    public static final String NAME = "heatmap";
    public static final Type TYPE = new Type(NAME);
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);
    public static final ParseField GEOM_FIELD = new ParseField("geom");
    public static final ParseField MAX_CELLS_FIELD = new ParseField("max_cells");
    public static final ParseField DIST_ERR_FIELD = new ParseField("dist_err");
    public static final ParseField DIST_ERR_PCT_FIELD = new ParseField("dist_err_pct");
    public static final ParseField GRID_LEVEL_FIELD = new ParseField("grid_level");

    private QueryBuilder geom;
    private Double distErr;
    private Double distErrPct;
    private Integer gridLevel;
    private Integer maxCells;
    private String field;

    /**
     * Creates a blank builder
     * 
     * @param name
     *            the name that was given this aggregation instance
     */
    public GeoHeatmapAggregationBuilder(String name) {
        super(name, TYPE);
    }

    /**
     * A utility method to construct a blank builder through the Java API
     * 
     * @param name
     *            a name for the aggregator instance
     * @return a new blank builder
     */
    public static GeoHeatmapAggregationBuilder heatmap(String name) {
        return new GeoHeatmapAggregationBuilder(name);
    }

    /**
     * Read from a stream
     */
    public GeoHeatmapAggregationBuilder(StreamInput in) throws IOException {
        super(in, TYPE);
        field = in.readString();
        geom = in.readOptionalNamedWriteable(QueryBuilder.class);
        distErr = in.readOptionalDouble();
        distErrPct = in.readOptionalDouble();
        gridLevel = in.readOptionalVInt();
        maxCells = in.readOptionalVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalNamedWriteable(geom);
        out.writeOptionalDouble(distErr);
        out.writeOptionalDouble(distErrPct);
        out.writeOptionalVInt(gridLevel);
        out.writeOptionalVInt(maxCells);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Construct a builder from XContent, which usually comes from the JSON
     * query API
     */
    public static GeoHeatmapAggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        XContentParser parser = context.parser();

        XContentParser.Token token = null;
        String currentFieldName = null;
        String field = null;
        GeoShapeQueryBuilder geom = null;
        Integer maxCells = null;
        Double distErr = null;
        Double distErrPct = null;
        Integer gridLevel = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if (context.getParseFieldMatcher().match(currentFieldName, DIST_ERR_FIELD)) {
                    distErr = DistanceUnit.parse(parser.text(), DistanceUnit.DEFAULT, DistanceUnit.DEFAULT);
                }
            } else if (token.isValue()) {
                if (context.getParseFieldMatcher().match(currentFieldName, MAX_CELLS_FIELD)) {
                    maxCells = parser.intValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, DIST_ERR_PCT_FIELD)) {
                    distErrPct = parser.doubleValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, GRID_LEVEL_FIELD)) {
                    gridLevel = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.getParseFieldMatcher().match(currentFieldName, GEOM_FIELD)) {
                    geom = (GeoShapeQueryBuilder) context.parseInnerQueryBuilder()
                            .filter(qb -> qb.getWriteableName().equals(GeoShapeQueryBuilder.NAME)).orElse(null);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_NULL) {
                if (context.getParseFieldMatcher().match(currentFieldName, MAX_CELLS_FIELD)
                        || context.getParseFieldMatcher().match(currentFieldName, DIST_ERR_PCT_FIELD)
                        || context.getParseFieldMatcher().match(currentFieldName, GRID_LEVEL_FIELD)
                        || context.getParseFieldMatcher().match(currentFieldName, GEOM_FIELD)) {
                    continue;
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }                
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            }
        }

        if (field == null) {
            throw new ParsingException(null, "Missing required field [field] for geo_heatmap aggregation [" + aggregationName + "]");
        }
        return new GeoHeatmapAggregationBuilder(aggregationName).geom(geom).field(field).maxCells(maxCells)
                .distErr(distErr).distErrPct(distErrPct).gridLevel(gridLevel);
    }

    @Override
    protected AggregatorFactory<?> doBuild(AggregationContext context, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
            throws IOException {
        Shape inputShape = null;
        if (geom != null) {
            GeoShapeQueryBuilder shapeBuilder = (GeoShapeQueryBuilder) geom;
            inputShape = shapeBuilder.shape().build();
        }
        GeoHeatmapAggregatorFactory factory = new GeoHeatmapAggregatorFactory(name, type, field, Optional.ofNullable(inputShape),
                Optional.ofNullable(maxCells), Optional.ofNullable(distErr), Optional.ofNullable(distErrPct),
                Optional.ofNullable(gridLevel), context, parent, subFactoriesBuilder, metaData);
        return factory;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (geom != null) {
            builder.field(GEOM_FIELD.getPreferredName(), geom);
        }
        builder.field("field", field);
        builder.field(MAX_CELLS_FIELD.getPreferredName(), maxCells);
        if (distErr != null) {
            builder.field(DIST_ERR_FIELD.getPreferredName(), distErr);
        }
        builder.field(DIST_ERR_PCT_FIELD.getPreferredName(), distErrPct);
        if (gridLevel != null) {
            builder.field(GRID_LEVEL_FIELD.getPreferredName(), gridLevel);
        }
        builder.endObject();
        return builder;
    }

    /**
     * @param field
     *            the field on which to build the heatmap; must be a geo_shape
     *            field.
     * @return this builder
     */
    public GeoHeatmapAggregationBuilder field(String field) {
        if (field == null) {
            throw new IllegalArgumentException("[field] must not be null: [" + name + "]");
        }
        this.field = field;
        return this;
    }

    /**
     * Sets a bounding geometry for the heatmap. The heatmap itself will be
     * rectangular, but hits outside of this geometry will not be counted
     * 
     * @param geom
     *            the bounding geometry; defaults to the world rectangle
     * @return this builder
     */
    public GeoHeatmapAggregationBuilder geom(GeoShapeQueryBuilder geom) {
        this.geom = geom;
        return this;
    }

    /**
     * Sets the maximum allowable error for determining where an indexed shape is
     * relative to the heatmap cells
     * 
     * @param distErr
     *            The distance in meters
     * @return this builder
     */
    public GeoHeatmapAggregationBuilder distErr(Double distErr) {
        if (distErr != null) this.distErr = distErr;
        return this;
    }
    
    /**
     * Sets the maximum allowable error for determining where an indexed shape is
     * relative to the heatmap cells, specified as a fraction of the shape size
     * 
     * @param distErrPct 
     *            A fraction from 0.0 to 0.5
     * @return this builder
     */
    public GeoHeatmapAggregationBuilder distErrPct(Double distErrPct) {
        if (distErrPct != null) this.distErrPct = distErrPct;
        return this;
    }
    
    /**
     * Sets the grid level (granularity) of the heatmap
     * 
     * @param gridLevel
     *            higher numbers mean higher granularity; defaults to 7
     * @return this builder
     */
    public GeoHeatmapAggregationBuilder gridLevel(Integer gridLevel) {
        if (gridLevel != null) this.gridLevel = gridLevel;
        return this;
    }

    /**
     * Set the maximum number of cells that can be returned in the heatmap
     * 
     * @param maxCells
     *            defaults to 100,000
     * @return this builder
     */
    public GeoHeatmapAggregationBuilder maxCells(Integer maxCells) {
        if (maxCells != null) this.maxCells = maxCells;
        return this;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, geom, distErr, distErrPct, gridLevel, maxCells);
    }

    @Override
    protected boolean doEquals(Object obj) {
        GeoHeatmapAggregationBuilder other = (GeoHeatmapAggregationBuilder) obj;
        return Objects.equals(field, other.field)
                && Objects.equals(geom, other.geom)
                && Objects.equals(distErr, other.distErr)
                && Objects.equals(distErrPct, other.distErrPct)
                && Objects.equals(gridLevel, other.gridLevel)
                && Objects.equals(maxCells, other.maxCells);
    }
}
