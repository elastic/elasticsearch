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

import org.elasticsearch.common.geo.DimensionalShapeType;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoRelation;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.fielddata.MultiGeoValues;

/**
 * A bounded representation of {@link UnboundedGeoShapeCellValues}
 */
class BoundedGeoShapeCellValues extends UnboundedGeoShapeCellValues {
    private BoundedGeoValue boundedGeoValue;

    protected BoundedGeoShapeCellValues(MultiGeoValues geoValues, int precision, GeoGridTiler tiler, GeoBoundingBox geoBoundingBox) {
        super(geoValues, precision, tiler);
        this.boundedGeoValue = new BoundedGeoValue(geoBoundingBox);
    }

    @Override
    int advanceValue(MultiGeoValues.GeoValue target, int valuesIdx) {
        boundedGeoValue.reset(target);
        return super.advanceValue(boundedGeoValue, valuesIdx);
    }

    static class BoundedGeoValue implements MultiGeoValues.GeoValue {
        private MultiGeoValues.GeoValue innerValue;
        private final double boundsTop;
        private final double boundsBottom;
        private final double boundsWestLeft;
        private final double boundsWestRight;
        private final double boundsEastLeft;
        private final double boundsEastRight;
        private final boolean crossesDateline;

        BoundedGeoValue(GeoBoundingBox geoBoundingBox) {
            this.innerValue = null;

            // split geoBoundingBox into west and east boxes
            boundsTop = geoBoundingBox.top();
            boundsBottom = geoBoundingBox.bottom();
            if (geoBoundingBox.right() < geoBoundingBox.left()) {
                boundsWestLeft = -180;
                boundsWestRight = geoBoundingBox.right();
                boundsEastLeft = geoBoundingBox.left();
                boundsEastRight = 180;
                crossesDateline = true;
            } else { // only set east bounds
                boundsEastLeft = geoBoundingBox.left();
                boundsEastRight = geoBoundingBox.right();
                boundsWestLeft = 0;
                boundsWestRight = 0;
                crossesDateline = false;
            }
        }

        void reset(MultiGeoValues.GeoValue innerValue) {
            this.innerValue = innerValue;
        }

        @Override
        public GeoRelation relate(Rectangle rectangle) {
            if (boundsTop >= rectangle.getMinY() && boundsBottom <= rectangle.getMaxY()
                    && (boundsEastLeft <= rectangle.getMaxX() && boundsEastRight >= rectangle.getMinX()
                    || (crossesDateline && boundsWestLeft <= rectangle.getMaxX() && boundsWestRight >= rectangle.getMinX()))) {
                GeoRelation relation = innerValue.relate(rectangle);
                // bounded relations can never be sure to be inside until each tile and sub-tile is checked
                // explicitly against the geoBoundingBox
                if (relation == GeoRelation.QUERY_INSIDE) {
                    return GeoRelation.QUERY_CROSSES;
                }
                return relation;
            } else {
                return GeoRelation.QUERY_DISJOINT;
            }
        }

        @Override
        public double lat() {
            return innerValue.lat();
        }

        @Override
        public double lon() {
            return innerValue.lon();
        }

        @Override
        public MultiGeoValues.BoundingBox boundingBox() {
            return innerValue.boundingBox();
        }

        @Override
        public DimensionalShapeType dimensionalShapeType() {
            return innerValue.dimensionalShapeType();
        }

        @Override
        public double weight() {
            return innerValue.weight();
        }
    }
}
