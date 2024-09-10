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
        var map = new HashMap<String, Object>();
        map.put("store", ESTestCase.randomBoolean());
        map.put("index", ESTestCase.randomBoolean());
        if (ESTestCase.randomBoolean()) {
            map.put("synthetic_source_keep", ESTestCase.randomFrom("none", "arrays", "all"));
        }
        return new DataSourceResponse.LeafMappingParametersGenerator(switch (request.fieldType()) {
            case KEYWORD -> keywordMapping(map);
            case LONG, INTEGER, SHORT, BYTE, DOUBLE, FLOAT, HALF_FLOAT -> numberMapping(map);
            case UNSIGNED_LONG -> unsignedLongMapping(map);
            case SCALED_FLOAT -> scaledFloatMapping(map);
        });
    }

    // TODO enable doc_values: false
    // It is disabled because it hits a bug in synthetic source.
    private Supplier<Map<String, Object>> keywordMapping(Map<String, Object> injected) {
        return () -> injected;
    }

    private Supplier<Map<String, Object>> numberMapping(Map<String, Object> injected) {
        return () -> {
           injected.put("doc_values", ESTestCase.randomBoolean());
           return injected;
        };
    }

    private Supplier<Map<String, Object>> unsignedLongMapping(Map<String, Object> injected) {
        return () -> injected;
    }

    private Supplier<Map<String, Object>> scaledFloatMapping(Map<String, Object> injected) {
        return () -> {
            injected.put("scaling_factor", ESTestCase.randomFrom(10, 1000, 100000, 100.5));
            return injected;
        };
    }

    @Override
    public DataSourceResponse.ObjectMappingParametersGenerator handle(DataSourceRequest.ObjectMappingParametersGenerator request) {
        if (request.isNested()) {
            return new DataSourceResponse.ObjectMappingParametersGenerator(() -> {
                var parameters = new HashMap<String, Object>();
                if (ESTestCase.randomBoolean()) {
                    parameters.put("dynamic", ESTestCase.randomFrom("true", "false", "strict"));
                }

                return parameters;
            });
        }

        return new DataSourceResponse.ObjectMappingParametersGenerator(() -> {
            var parameters = new HashMap<String, Object>();
            if (ESTestCase.randomBoolean()) {
                parameters.put("dynamic", ESTestCase.randomFrom("true", "false", "strict", "runtime"));
            }
            if (ESTestCase.randomBoolean()) {
                parameters.put("enabled", ESTestCase.randomFrom("true", "false"));
            }

            return parameters;
        });
    }
}
