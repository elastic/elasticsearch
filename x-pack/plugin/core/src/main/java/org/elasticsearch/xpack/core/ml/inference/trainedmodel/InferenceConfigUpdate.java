/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public interface InferenceConfigUpdate extends VersionedNamedWriteable {
    Set<String> RESERVED_ML_FIELD_NAMES = new HashSet<>(
        Arrays.asList(WarningInferenceResults.WARNING.getPreferredName(), TrainedModelConfig.MODEL_ID.getPreferredName())
    );

    InferenceConfig apply(InferenceConfig originalConfig);

    boolean isSupported(InferenceConfig config);

    String getResultsField();

    interface Builder<T extends Builder<T, U>, U extends InferenceConfigUpdate> {
        U build();

        T setResultsField(String resultsField);
    }

    Builder<? extends Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder();

    default String getName() {
        return getWriteableName();
    }

    static void checkFieldUniqueness(String... fieldNames) {
        Set<String> duplicatedFieldNames = new HashSet<>();
        Set<String> currentFieldNames = new HashSet<>(RESERVED_ML_FIELD_NAMES);
        for (String fieldName : fieldNames) {
            if (fieldName == null) {
                continue;
            }
            if (currentFieldNames.contains(fieldName)) {
                duplicatedFieldNames.add(fieldName);
            } else {
                currentFieldNames.add(fieldName);
            }
        }
        if (duplicatedFieldNames.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "Invalid inference config." + " More than one field is configured as {}",
                duplicatedFieldNames
            );
        }
    }
}
