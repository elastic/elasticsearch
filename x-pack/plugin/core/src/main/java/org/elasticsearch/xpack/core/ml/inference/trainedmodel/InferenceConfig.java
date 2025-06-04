/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.License;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.util.Arrays;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.ml.MachineLearningField.ML_API_FEATURE;

public interface InferenceConfig extends NamedXContentObject, VersionedNamedWriteable {

    String DEFAULT_TOP_CLASSES_RESULTS_FIELD = "top_classes";
    String DEFAULT_RESULTS_FIELD = "predicted_value";
    ParseField RESULTS_FIELD = new ParseField("results_field");

    boolean isTargetTypeSupported(TargetType targetType);

    /**
     * Return a copy of this with the settings updated by the
     * values in {@code update}.
     * @param update The update to apply
     * @return A new updated config
     */
    InferenceConfig apply(InferenceConfigUpdate update);

    @Override
    default TransportVersion getMinimalSupportedVersion() {
        return getMinimalSupportedTransportVersion();
    }

    /**
     * All nodes in the cluster must have at least this MlConfigVersion attribute
     */
    MlConfigVersion getMinimalSupportedMlConfigVersion();

    /**
     * All communication in the cluster must use at least this version
     */
    TransportVersion getMinimalSupportedTransportVersion();

    default boolean requestingImportance() {
        return false;
    }

    String getResultsField();

    boolean isAllocateOnly();

    default boolean supportsIngestPipeline() {
        return true;
    }

    default boolean supportsPipelineAggregation() {
        return true;
    }

    default boolean supportsSearchRescorer() {
        return false;
    }

    @Nullable
    default TrainedModelInput getDefaultInput(TrainedModelType modelType) {
        if (modelType == null) {
            return null;
        }
        return modelType.getDefaultInput();
    }

    default ActionRequestValidationException validateTrainedModelInput(
        TrainedModelInput input,
        boolean forCreation,
        ActionRequestValidationException validationException
    ) {

        if (input != null && input.getFieldNames().isEmpty()) {
            validationException = addValidationError("[input.field_names] must not be empty", validationException);
        }

        if (input != null
            && input.getFieldNames()
                .stream()
                .filter(s -> s.contains("."))
                .flatMap(s -> Arrays.stream(Strings.delimitedListToStringArray(s, ".")))
                .anyMatch(String::isEmpty)) {
            validationException = addValidationError(
                "[input.field_names] must only contain valid dot delimited field names",
                validationException
            );
        }

        return validationException;
    }

    default ElasticsearchStatusException incompatibleUpdateException(String updateName) {
        throw ExceptionsHelper.badRequestException(
            "Inference config of type [{}] can not be updated with a inference request of type [{}]",
            getName(),
            updateName
        );
    }

    default License.OperationMode getMinLicenseSupported() {
        return ML_API_FEATURE.getMinimumOperationMode();
    }

    default License.OperationMode getMinLicenseSupportedForAction(RestRequest.Method method) {
        return getMinLicenseSupported();
    }
}
