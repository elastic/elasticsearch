/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class DefaultMappingParametersHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
        return new DataSourceResponse.LeafMappingParametersGenerator(switch (request.fieldType()) {
            case KEYWORD -> keywordMapping();
            case LONG, INTEGER, SHORT, BYTE, DOUBLE, FLOAT, HALF_FLOAT -> numberMapping();
            case UNSIGNED_LONG -> unsignedLongMapping();
            case SCALED_FLOAT -> scaledFloatMapping();
        });
    }

    // TODO enable doc_values: false
    // It is disabled because it hits a bug in synthetic source.
    private Supplier<Map<String, Object>> keywordMapping() {
        return () -> Map.of("store", ESTestCase.randomBoolean(), "index", ESTestCase.randomBoolean());
    }

    private Supplier<Map<String, Object>> numberMapping() {
        return () -> Map.of(
            "store",
            ESTestCase.randomBoolean(),
            "index",
            ESTestCase.randomBoolean(),
            "doc_values",
            ESTestCase.randomBoolean()
        );
    }

    private Supplier<Map<String, Object>> unsignedLongMapping() {
        return () -> Map.of("store", ESTestCase.randomBoolean(), "index", ESTestCase.randomBoolean());
    }

    private Supplier<Map<String, Object>> scaledFloatMapping() {
        return () -> {
            var scalingFactor = ESTestCase.randomFrom(10, 1000, 100000, 100.5);
            return Map.of("scaling_factor", scalingFactor, "store", ESTestCase.randomBoolean(), "index", ESTestCase.randomBoolean());
        };
    }

    @Override
    public DataSourceResponse.ObjectMappingParametersGenerator handle(DataSourceRequest.ObjectMappingParametersGenerator request) {
        if (request.isNested()) {
            return new DataSourceResponse.ObjectMappingParametersGenerator(
                // TODO enable "false" and "strict"
                // It is disabled because it hits a bug in synthetic source.
                () -> {
                    var parameters = new HashMap<String, Object>();
                    if (ESTestCase.randomBoolean()) {
                        parameters.put("dynamic", "true");
                    }

                    return parameters;
                }
            );
        }

        // TODO enable "enabled: false" and "dynamic: false/runtime"
        // It is disabled because it hits a bug in synthetic source.
        return new DataSourceResponse.ObjectMappingParametersGenerator(() -> {
            var parameters = new HashMap<String, Object>();
            if (ESTestCase.randomBoolean()) {
                parameters.put("dynamic", ESTestCase.randomFrom("true", "strict"));
            }
            if (ESTestCase.randomBoolean()) {
                parameters.put("enabled", "true");
            }

            return parameters;
        });
    }
}
