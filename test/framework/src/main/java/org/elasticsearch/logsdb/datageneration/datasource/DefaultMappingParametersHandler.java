/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.logsdb.datageneration.fields.DynamicMapping;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DefaultMappingParametersHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
        var map = new HashMap<String, Object>();
        map.put("store", ESTestCase.randomBoolean());
        map.put("index", ESTestCase.randomBoolean());
        map.put("doc_values", ESTestCase.randomBoolean());
        if (ESTestCase.randomBoolean()) {
            map.put(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, ESTestCase.randomFrom("none", "arrays", "all"));
        }

        return new DataSourceResponse.LeafMappingParametersGenerator(switch (request.fieldType()) {
            case KEYWORD -> keywordMapping(request, map);
            case LONG, INTEGER, SHORT, BYTE, DOUBLE, FLOAT, HALF_FLOAT, UNSIGNED_LONG -> plain(map);
            case SCALED_FLOAT -> scaledFloatMapping(map);
        });
    }

    private Supplier<Map<String, Object>> plain(Map<String, Object> injected) {
        return () -> injected;
    }

    private Supplier<Map<String, Object>> keywordMapping(
        DataSourceRequest.LeafMappingParametersGenerator request,
        Map<String, Object> injected
    ) {
        return () -> {
            // Inject copy_to sometimes but reflect that it is not widely used in reality.
            // We only add copy_to to keywords because we get into trouble with numeric fields that are copied to dynamic fields.
            // If first copied value is numeric, dynamic field is created with numeric field type and then copy of text values fail.
            // Actual value being copied does not influence the core logic of copy_to anyway.
            //
            // TODO
            // We don't use copy_to on fields that are inside an object with dynamic: strict
            // because we'll hit https://github.com/elastic/elasticsearch/issues/113049.
            if (request.dynamicMapping() != DynamicMapping.FORBIDDEN && ESTestCase.randomDouble() <= 0.05) {
                var options = request.eligibleCopyToFields()
                    .stream()
                    .filter(f -> f.equals(request.fieldName()) == false)
                    .collect(Collectors.toSet());

                if (options.isEmpty() == false) {
                    injected.put("copy_to", ESTestCase.randomFrom(options));
                }
            }

            return injected;
        };
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
                if (ESTestCase.randomBoolean()) {
                    parameters.put(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "all");  // [arrays] doesn't apply to nested objects
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
            if (ESTestCase.randomBoolean()) {
                parameters.put(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, request.syntheticSourceKeepValue());
            }

            return parameters;
        });
    }
}
