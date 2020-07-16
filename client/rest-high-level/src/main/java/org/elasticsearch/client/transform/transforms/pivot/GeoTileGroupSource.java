/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/*
 * A geotile_grid aggregation source for group_by
 */
public class GeoTileGroupSource extends SingleGroupSource implements ToXContentObject {
    private static final String NAME = "transform_geo_tile_group";

    private static final ParseField PRECISION = new ParseField("precision");
    private static final ConstructingObjectParser<GeoTileGroupSource, Void> PARSER = new ConstructingObjectParser<>(NAME, true,  (args) -> {
        String field = (String) args[0];
        Integer precision = (Integer) args[1];
        GeoBoundingBox boundingBox = (GeoBoundingBox) args[2];

        return new GeoTileGroupSource(field, precision, boundingBox);
    });

    static {
        PARSER.declareString(optionalConstructorArg(), FIELD);
        PARSER.declareInt(optionalConstructorArg(), PRECISION);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, context) -> GeoBoundingBox.parseBoundingBox(p),
            GeoBoundingBox.BOUNDS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
    }
    private final Integer precision;
    private final GeoBoundingBox geoBoundingBox;

    public GeoTileGroupSource(final String field, final Integer precision, final GeoBoundingBox boundingBox) {
        super(field, null);
        if (precision != null) {
            GeoTileUtils.checkPrecisionRange(precision);
        }
        this.precision = precision;
        this.geoBoundingBox = boundingBox;
    }

    @Override
    public Type getType() {
        return Type.GEOTILE_GRID;
    }

    public Integer getPrecision() {
        return precision;
    }

    public GeoBoundingBox getGeoBoundingBox() {
        return geoBoundingBox;
    }

    public static GeoTileGroupSource fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.innerXContent(builder, params);
        if (precision != null) {
            builder.field(PRECISION.getPreferredName(), precision);
        }
        if (geoBoundingBox != null) {
            geoBoundingBox.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final GeoTileGroupSource that = (GeoTileGroupSource) other;

        return Objects.equals(this.field, that.field)
            && Objects.equals(this.precision, that.precision)
            && Objects.equals(this.geoBoundingBox, that.geoBoundingBox);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, precision, geoBoundingBox);
    }

}
