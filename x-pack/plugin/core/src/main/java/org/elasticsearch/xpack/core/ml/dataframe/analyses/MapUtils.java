/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *//*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class MapUtils {

    private static final Map<String, Object> FEATURE_IMPORTANCE_MAPPING;
    static {
        Map<String, Object> featureImportanceMappingProperties = new HashMap<>();
        featureImportanceMappingProperties.put("feature_name", Collections.singletonMap("type", KeywordFieldMapper.CONTENT_TYPE));
        featureImportanceMappingProperties.put("importance",
            Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));
        Map<String, Object> featureImportanceMapping = new HashMap<>();
        // TODO sorted indices don't support nested types
        //featureImportanceMapping.put("dynamic", true);
        //featureImportanceMapping.put("type", ObjectMapper.NESTED_CONTENT_TYPE);
        featureImportanceMapping.put("properties", featureImportanceMappingProperties);
        FEATURE_IMPORTANCE_MAPPING = Collections.unmodifiableMap(featureImportanceMapping);
    }

    static Map<String, Object> featureImportanceMapping() {
        return FEATURE_IMPORTANCE_MAPPING;
    }

    private MapUtils() {}
}
