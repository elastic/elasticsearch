/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.fields.leaf;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datageneration.FieldDataGenerator;
import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class GeoPointFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> formattedPoints;
    private final Supplier<Object> formattedPointsWithMalformed;

    public GeoPointFieldDataGenerator(DataSource dataSource) {
        var points = dataSource.get(new DataSourceRequest.GeoPointGenerator()).generator();
        var representations = dataSource.get(
            new DataSourceRequest.TransformWeightedWrapper<GeoPoint>(
                List.of(
                    Tuple.tuple(0.2, p -> Map.of("type", "point", "coordinates", List.of(p.getLon(), p.getLat()))),
                    Tuple.tuple(0.2, p -> "POINT( " + p.getLon() + " " + p.getLat() + " )"),
                    Tuple.tuple(0.2, p -> Map.of("lon", p.getLon(), "lat", p.getLat())),
                    // this triggers a bug in stored source block loader, see #125710
                    // Tuple.tuple(0.2, p -> List.of(p.getLon(), p.getLat())),
                    Tuple.tuple(0.2, p -> p.getLat() + "," + p.getLon()),
                    Tuple.tuple(0.2, GeoPoint::getGeohash)
                )
            )
        );

        var pointRepresentations = representations.wrapper().apply(points);

        this.formattedPoints = Wrappers.defaults(pointRepresentations, dataSource);

        var strings = dataSource.get(new DataSourceRequest.StringGenerator()).generator();
        this.formattedPointsWithMalformed = Wrappers.defaultsWithMalformed(pointRepresentations, strings::get, dataSource);
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        if (fieldMapping == null) {
            // dynamically mapped and dynamic mapping does not play well with this type (it sometimes gets mapped as an object)
            // return null to skip indexing this field
            return null;
        }

        if ((Boolean) fieldMapping.getOrDefault("ignore_malformed", false)) {
            return formattedPointsWithMalformed.get();
        }

        return formattedPoints.get();
    }
}
