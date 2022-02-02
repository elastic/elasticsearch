/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SourceDestValidation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_IN_SOURCE_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_PIPELINE_MISSING_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_SINGLE_INDEX_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SOURCE_MISSING_VALIDATION;

/**
 * Packs together useful sets of validations in the context transforms
 */
public final class SourceDestValidations {

    private SourceDestValidations() {}

    private static final SourceDestValidation REMOTE_SOURCE_VALIDATION =
        new SourceDestValidator.RemoteSourceEnabledAndRemoteLicenseValidation("transform");

    private static final List<SourceDestValidation> PREVIEW_VALIDATIONS = Arrays.asList(
        SOURCE_MISSING_VALIDATION,
        REMOTE_SOURCE_VALIDATION,
        DESTINATION_PIPELINE_MISSING_VALIDATION
    );

    private static final List<SourceDestValidation> ALL_VALIDATIONS = Arrays.asList(
        SOURCE_MISSING_VALIDATION,
        REMOTE_SOURCE_VALIDATION,
        DESTINATION_IN_SOURCE_VALIDATION,
        DESTINATION_SINGLE_INDEX_VALIDATION,
        DESTINATION_PIPELINE_MISSING_VALIDATION
    );

    private static final List<SourceDestValidation> NON_DEFERABLE_VALIDATIONS = Collections.singletonList(
        DESTINATION_SINGLE_INDEX_VALIDATION
    );

    public static List<SourceDestValidation> getValidations(boolean isDeferValidation, List<SourceDestValidation> additionalValidations) {
        return getValidations(isDeferValidation, ALL_VALIDATIONS, additionalValidations);
    }

    public static List<SourceDestValidation> getValidationsForPreview(List<SourceDestValidation> additionalValidations) {
        return getValidations(false, PREVIEW_VALIDATIONS, additionalValidations);
    }

    private static List<SourceDestValidation> getValidations(
        boolean isDeferValidation,
        List<SourceDestValidation> primaryValidations,
        List<SourceDestValidation> additionalValidations
    ) {
        if (isDeferValidation) {
            return SourceDestValidations.NON_DEFERABLE_VALIDATIONS;
        }
        if (additionalValidations.isEmpty()) {
            return primaryValidations;
        }
        List<SourceDestValidation> validations = new ArrayList<>(primaryValidations);
        validations.addAll(additionalValidations);
        return validations;
    }
}
