/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.spatial.datageneration;

import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.datasource.DataSource;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;

import java.util.Map;
import java.util.function.Supplier;

public class GeoShapeFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> formattedGeoShapes;

    public GeoShapeFieldDataGenerator(DataSource dataSource) {
        var geoShapes = dataSource.get(new DataSourceRequest.GeoShapeGenerator()).generator();
        var serializeToGeoJson = dataSource.get(new DataSourceRequest.TransformWrapper(0.5, g -> GeoJson.toMap((Geometry) g)));

        this.formattedGeoShapes = serializeToGeoJson.wrapper().andThen(values -> (Supplier<Object>) () -> {
            var value = values.get();
            if (value instanceof Geometry g) {
                // did not transform
                return WellKnownText.toWKT(g);
            }
            return value;
        }).apply(geoShapes::get);

    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        return formattedGeoShapes.get();
    }
}
