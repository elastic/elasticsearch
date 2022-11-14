/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

public class H3PolygonScaleRecommenderSphericalTests extends H3PolygonScaleRecommenderTests {

    @Override
    protected H3LatLonGeometry makeGeometry(String h3Address) {
        return new H3LatLonGeometry.Spherical(h3Address);
    }

    @Override
    protected H3LatLonGeometry makeGeometry(String h3Address, double scaleFactor) {
        return new H3LatLonGeometry.Spherical.Scaled(h3Address, scaleFactor);
    }

    @Override
    protected double getLatitudeThreshold() {
        return 90d;
    }

    @Override
    protected H3PolygonScaleRecommender scaleRecommender() {
        return H3PolygonScaleRecommender.SPHERICAL;
    }
}
