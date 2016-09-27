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

/**
 * Collects the various parameters for a heatmap aggregation and builds a
 * factory
 */
public class GeoHeatmapAggregationBuilder extends AbstractAggregationBuilder<GeoHeatmapAggregationBuilder> {
    public static final String NAME = "heatmap";
    public static final Type TYPE = new Type(NAME);
    public static final int DEFAULT_GRID_LEVEL = 7;
    public static final int DEFAULT_MAX_CELLS = 100_000;
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);
    public static final ParseField GEOM = new ParseField("geom");
    public static final ParseField MAX_CELLS = new ParseField("max_cells");
    public static final ParseField GRID_LEVEL = new ParseField("grid_level");

    private QueryBuilder geom;
    private int gridLevel = DEFAULT_GRID_LEVEL;
    private int maxCells = DEFAULT_MAX_CELLS;
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
        gridLevel = in.readInt();
        maxCells = in.readInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalNamedWriteable(geom);
        out.writeInt(gridLevel);
        out.writeInt(maxCells);
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
        int maxCells = 0;
        int gridLevel = 0;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                }
            } else if (token.isValue()) {
                if (context.getParseFieldMatcher().match(currentFieldName, MAX_CELLS)) {
                    maxCells = parser.intValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, GRID_LEVEL)) {
                    gridLevel = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.getParseFieldMatcher().match(currentFieldName, GEOM)) {
                    geom = (GeoShapeQueryBuilder) context.parseInnerQueryBuilder()
                            .filter(qb -> qb.getWriteableName().equals(GeoShapeQueryBuilder.NAME)).orElse(null);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            }
        }

        if (geom == null) {
            throw new ParsingException(null, "Missing required field [geom] for geo_heatmap aggregation [" + aggregationName + "]");
        }
        if (field == null) {
            throw new ParsingException(null, "Missing required field [field] for geo_heatmap aggregation [" + aggregationName + "]");
        }
        return new GeoHeatmapAggregationBuilder(aggregationName).geom(geom).field(field).maxCells(maxCells).gridLevel(gridLevel);
    }

    @Override
    protected AggregatorFactory<?> doBuild(AggregationContext context, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
            throws IOException {
        Shape inputShape = null;
        if (geom != null) {
            GeoShapeQueryBuilder shapeBuilder = (GeoShapeQueryBuilder) geom;
            inputShape = shapeBuilder.shape().build();
        }
        GeoHeatmapAggregatorFactory factory = new GeoHeatmapAggregatorFactory(name, type, field, inputShape, maxCells, gridLevel, context,
                parent, subFactoriesBuilder, metaData);
        return factory;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (geom != null) {
            builder.field(GEOM.getPreferredName(), geom);
        }
        builder.field("field", field);
        builder.field(MAX_CELLS.getPreferredName(), maxCells);
        builder.field(GRID_LEVEL.getPreferredName(), gridLevel);
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
     * Sets the grid level (granularity) of the heatmap
     * 
     * @param gridLevel
     *            higher numbers mean higher granularity; defaults to 7
     * @return this builder
     */
    public GeoHeatmapAggregationBuilder gridLevel(int gridLevel) {
        this.gridLevel = gridLevel;
        return this;
    }

    /**
     * Set the maximum number of cells that can be returned in the heatmap
     * 
     * @param maxCells
     *            defaults to 100,000
     * @return this builder
     */
    public GeoHeatmapAggregationBuilder maxCells(int maxCells) {
        this.maxCells = maxCells;
        return this;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, geom, gridLevel, maxCells);
    }

    @Override
    protected boolean doEquals(Object obj) {
        GeoHeatmapAggregationBuilder other = (GeoHeatmapAggregationBuilder) obj;
        return Objects.equals(field, other.field) && Objects.equals(geom, other.geom) && Objects.equals(gridLevel, other.gridLevel)
                && Objects.equals(maxCells, other.maxCells);
    }
}
