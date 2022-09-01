/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.spatial3d.geom;

/**
 * Class which constructs a GeoPolygon representing a regular simple convex polygon, for example an S2 google pixel, or an H3 uber tile.
 */
public class GeoRegularConvexPolygonFactory {

    private GeoRegularConvexPolygonFactory() {}

    /**
     * Creates a convex polygon with N planes by providing N points in CCW. This is a very fast shape
     * and there are no checks that the points currently define a convex shape. The last point should not be a copy of the first point.
     *
     * @param planetModel The planet model
     * @param points an array of at least three points in CCW orientation.
     * @return the generated shape.
     */
    public static GeoPolygon makeGeoPolygon(final PlanetModel planetModel, final GeoPoint... points) {
        return new GeoRegularConvexPolygon(planetModel, points);
    }
}
