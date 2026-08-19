/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SourceDestValidation;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.ArrayList;
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

    static final SourceDestValidation REMOTE_SOURCE_VALIDATION = new SourceDestValidator.RemoteSourceEnabledAndRemoteLicenseValidation(
        "transform"
    );

    private static final List<SourceDestValidation> PREVIEW_VALIDATIONS_FOR_CPS = List.of(DESTINATION_PIPELINE_MISSING_VALIDATION);

    private static final List<SourceDestValidation> PREVIEW_VALIDATIONS = List.of(
        SOURCE_MISSING_VALIDATION,
        REMOTE_SOURCE_VALIDATION,
        DESTINATION_PIPELINE_MISSING_VALIDATION
    );

    // SOURCE_MISSING and REMOTE_SOURCE license check are deferred to the runtime search call.
    // DESTINATION_IN_SOURCE is skipped because project_routing determines at search time
    // whether dest is included in the cross-project source expression.
    private static final List<SourceDestValidation> ALL_VALIDATIONS_FOR_CPS = List.of(
        DESTINATION_SINGLE_INDEX_VALIDATION,
        DESTINATION_PIPELINE_MISSING_VALIDATION
    );

    private static final List<SourceDestValidation> ALL_VALIDATIONS = List.of(
        SOURCE_MISSING_VALIDATION,
        REMOTE_SOURCE_VALIDATION,
        DESTINATION_IN_SOURCE_VALIDATION,
        DESTINATION_SINGLE_INDEX_VALIDATION,
        DESTINATION_PIPELINE_MISSING_VALIDATION
    );

    private static final List<SourceDestValidation> NON_DEFERABLE_VALIDATIONS = Collections.singletonList(
        DESTINATION_SINGLE_INDEX_VALIDATION
    );

    public static List<SourceDestValidation> getValidations(
        boolean isDeferValidation,
        CrossProjectModeDecider crossProjectModeDecider,
        List<SourceDestValidation> additionalValidations
    ) {
        return getValidations(
            isDeferValidation,
            isCrossProjectSource(crossProjectModeDecider) ? ALL_VALIDATIONS_FOR_CPS : ALL_VALIDATIONS,
            additionalValidations
        );
    }

    public static List<SourceDestValidation> getValidationsForPreview(
        CrossProjectModeDecider crossProjectModeDecider,
        List<SourceDestValidation> additionalValidations
    ) {
        return getValidations(
            false,
            isCrossProjectSource(crossProjectModeDecider) ? PREVIEW_VALIDATIONS_FOR_CPS : PREVIEW_VALIDATIONS,
            additionalValidations
        );
    }

    private static boolean isCrossProjectSource(CrossProjectModeDecider crossProjectModeDecider) {
        return crossProjectModeDecider.crossProjectEnabled() && TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled();
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
