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

    private static Map<String, Object> createFeatureImportanceMapping(Map<String, Object> featureImportanceMappingProperties){
        featureImportanceMappingProperties.put("feature_name", Collections.singletonMap("type", KeywordFieldMapper.CONTENT_TYPE));
        Map<String, Object> featureImportanceMapping = new HashMap<>();
        // TODO sorted indices don't support nested types
        //featureImportanceMapping.put("dynamic", true);
        //featureImportanceMapping.put("type", ObjectMapper.NESTED_CONTENT_TYPE);
        featureImportanceMapping.put("properties", featureImportanceMappingProperties);
        return featureImportanceMapping;
    }

    private static final Map<String, Object> CLASSIFICATION_FEATURE_IMPORTANCE_MAPPING;
    static {
        Map<String, Object> classImportancePropertiesMapping = new HashMap<>();
        // TODO sorted indices don't support nested types
        //classImportancePropertiesMapping.put("dynamic", true);
        //classImportancePropertiesMapping.put("type", ObjectMapper.NESTED_CONTENT_TYPE);
        classImportancePropertiesMapping.put("class_name", Collections.singletonMap("type", KeywordFieldMapper.CONTENT_TYPE));
        classImportancePropertiesMapping.put("importance",
            Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));
        Map<String, Object> featureImportancePropertiesMapping = new HashMap<>();
        featureImportancePropertiesMapping.put("classes", Collections.singletonMap("properties", classImportancePropertiesMapping));
        CLASSIFICATION_FEATURE_IMPORTANCE_MAPPING =
            Collections.unmodifiableMap(createFeatureImportanceMapping(featureImportancePropertiesMapping));
    }

    private static final Map<String, Object> REGRESSION_FEATURE_IMPORTANCE_MAPPING;
    static {
        Map<String, Object> featureImportancePropertiesMapping = new HashMap<>();
        featureImportancePropertiesMapping.put("importance",
            Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));
        REGRESSION_FEATURE_IMPORTANCE_MAPPING =
            Collections.unmodifiableMap(createFeatureImportanceMapping(featureImportancePropertiesMapping));
    }

    static Map<String, Object> regressionFeatureImportanceMapping() {
        return REGRESSION_FEATURE_IMPORTANCE_MAPPING;
    }

    static Map<String, Object> classificationFeatureImportanceMapping() {
        return CLASSIFICATION_FEATURE_IMPORTANCE_MAPPING;
    }

    private MapUtils() {}
}
