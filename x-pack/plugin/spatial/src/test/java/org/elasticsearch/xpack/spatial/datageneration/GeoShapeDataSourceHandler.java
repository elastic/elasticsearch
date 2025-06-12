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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.util.HashMap;

public class GeoShapeDataSourceHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.GeoShapeGenerator handle(DataSourceRequest.GeoShapeGenerator request) {
        return new DataSourceResponse.GeoShapeGenerator(this::generateValidGeoShape);
    }

    @Override
    public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
        if (request.fieldType().equals("geo_shape") == false) {
            return null;
        }

        return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
            var map = new HashMap<String, Object>();
            map.put("store", ESTestCase.randomBoolean());
            map.put("index", ESTestCase.randomBoolean());
            map.put("doc_values", ESTestCase.randomBoolean());

            if (ESTestCase.randomBoolean()) {
                map.put("ignore_malformed", ESTestCase.randomBoolean());
            }

            return map;
        });
    }

    @Override
    public DataSourceResponse.FieldDataGenerator handle(DataSourceRequest.FieldDataGenerator request) {
        if (request.fieldType().equals("geo_shape") == false) {
            return null;
        }

        return new DataSourceResponse.FieldDataGenerator(new GeoShapeFieldDataGenerator(request.dataSource()));
    }

    private Geometry generateValidGeoShape() {
        while (true) {
            var geometry = GeometryTestUtils.randomGeometryWithoutCircle(0, false);
            if (geometry.type() == ShapeType.ENVELOPE) {
                // Not supported in GeoJson, skip it
                continue;
            }
            try {
                GeoTestUtils.binaryGeoShapeDocValuesField("f", geometry);
                return geometry;
            } catch (IllegalArgumentException ignored) {
                // Some generated shapes are not suitable for the storage format, ignore them
            }
        }
    }
}
