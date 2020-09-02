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
import org.elasticsearch.index.mapper.ObjectMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class MapUtils {

    private static final Map<String, Object> CLASSIFICATION_FEATURE_IMPORTANCE_MAPPING;
    static {
        Map<String, Object> classesProperties = new HashMap<>();
        classesProperties.put("class_name", Collections.singletonMap("type", KeywordFieldMapper.CONTENT_TYPE));
        classesProperties.put("importance", Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));

        Map<String, Object> classesMapping = new HashMap<>();
        classesMapping.put("dynamic", false);
        classesMapping.put("type", ObjectMapper.NESTED_CONTENT_TYPE);
        classesMapping.put("properties", classesProperties);

        Map<String, Object> properties = new HashMap<>();
        properties.put("feature_name", Collections.singletonMap("type", KeywordFieldMapper.CONTENT_TYPE));
        properties.put("importance", Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));
        properties.put("classes", classesMapping);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("dynamic", false);
        mapping.put("type", ObjectMapper.NESTED_CONTENT_TYPE);
        mapping.put("properties", properties);

        CLASSIFICATION_FEATURE_IMPORTANCE_MAPPING = Collections.unmodifiableMap(mapping);
    }

    private static final Map<String, Object> REGRESSION_FEATURE_IMPORTANCE_MAPPING;
    static {
        Map<String, Object> properties = new HashMap<>();
        properties.put("feature_name", Collections.singletonMap("type", KeywordFieldMapper.CONTENT_TYPE));
        properties.put("importance", Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("dynamic", false);
        mapping.put("type", ObjectMapper.NESTED_CONTENT_TYPE);
        mapping.put("properties", properties);

        REGRESSION_FEATURE_IMPORTANCE_MAPPING = Collections.unmodifiableMap(mapping);
    }

    static Map<String, Object> regressionFeatureImportanceMapping() {
        return REGRESSION_FEATURE_IMPORTANCE_MAPPING;
    }

    static Map<String, Object> classificationFeatureImportanceMapping() {
        return CLASSIFICATION_FEATURE_IMPORTANCE_MAPPING;
    }

    private MapUtils() {}
}
