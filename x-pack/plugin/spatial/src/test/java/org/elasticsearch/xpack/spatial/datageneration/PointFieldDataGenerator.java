/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.datageneration;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.datageneration.FieldDataGenerator;
import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.fields.leaf.Wrappers;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class PointFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> formattedPoints;
    private final Supplier<Object> formattedPointsWithMalformed;

    public PointFieldDataGenerator(DataSource dataSource) {
        var points = dataSource.get(new DataSourceRequest.PointGenerator()).generator();
        var representations = dataSource.get(
            new DataSourceRequest.TransformWeightedWrapper<CartesianPoint>(
                List.of(
                    Tuple.tuple(0.25, cp -> Map.of("type", "point", "coordinates", List.of(cp.getX(), cp.getY()))),
                    Tuple.tuple(0.25, cp -> "POINT( " + cp.getX() + " " + cp.getY() + " )"),
                    Tuple.tuple(0.25, cp -> Map.of("x", cp.getX(), "y", cp.getY())),
                    // this triggers a bug in stored source block loader, see #125710
                    // Tuple.tuple(0.2, cp -> List.of(cp.getX(), cp.getY())),
                    Tuple.tuple(0.25, cp -> cp.getX() + "," + cp.getY())
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
