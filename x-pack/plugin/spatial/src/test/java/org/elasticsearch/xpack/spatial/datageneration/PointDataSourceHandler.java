/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.datageneration;

import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.util.HashMap;
import java.util.Map;

public class PointDataSourceHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.PointGenerator handle(DataSourceRequest.PointGenerator request) {
        return new DataSourceResponse.PointGenerator(this::generateValidPoint);
    }

    @Override
    public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
        if (request.fieldType().equals("point") == false) {
            return null;
        }

        return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
            var map = new HashMap<String, Object>();
            map.put("index", ESTestCase.randomBoolean());
            map.put("doc_values", ESTestCase.randomBoolean());
            map.put("store", ESTestCase.randomBoolean());

            if (ESTestCase.randomBoolean()) {
                map.put("ignore_malformed", ESTestCase.randomBoolean());
            }

            if (ESTestCase.randomDouble() <= 0.2) {
                var point = generateValidPoint();

                map.put("null_value", Map.of("x", point.getX(), "y", point.getY()));
            }

            return map;
        });
    }

    @Override
    public DataSourceResponse.FieldDataGenerator handle(DataSourceRequest.FieldDataGenerator request) {
        if (request.fieldType().equals("point") == false) {
            return null;
        }

        return new DataSourceResponse.FieldDataGenerator(new PointFieldDataGenerator(request.dataSource()));
    }

    private CartesianPoint generateValidPoint() {
        var point = GeometryTestUtils.randomPoint(false);
        return new CartesianPoint(point.getLat(), point.getLon());
    }
}
