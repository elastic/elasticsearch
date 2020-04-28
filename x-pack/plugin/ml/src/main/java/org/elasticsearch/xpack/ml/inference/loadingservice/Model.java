/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

import java.util.Map;

public interface Model {

    String getResultsType();

    void infer(Map<String, Object> fields, InferenceConfigUpdate inferenceConfig, ActionListener<InferenceResults> listener);

    String getModelId();

    /**
     * Used for translating field names in according to the passed `fieldMappings` parameter.
     *
     * This mutates the `fields` parameter in-place.
     *
     * Fields are only appended. If the expected field name already exists, it is not created/overwritten.
     *
     * Original fields are not deleted.
     *
     * @param fields Fields to map against
     * @param fieldMapping Field originalName to expectedName string mapping
     */
    static void mapFieldsIfNecessary(Map<String, Object> fields, Map<String, String> fieldMapping) {
        if (fieldMapping != null) {
            fieldMapping.forEach((src, dest) -> {
                Object srcValue = MapHelper.dig(src, fields);
                if (srcValue != null) {
                    fields.putIfAbsent(dest, srcValue);
                }
            });
        }
    }

    InferenceStats getLatestStatsAndReset();
}
