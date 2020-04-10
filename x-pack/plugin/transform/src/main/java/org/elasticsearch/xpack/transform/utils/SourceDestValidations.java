/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_IN_SOURCE_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_SINGLE_INDEX_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.REMOTE_SOURCE_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SOURCE_MISSING_VALIDATION;

/**
 * Packs together useful sets of validations in the context transforms
 */
public final class SourceDestValidations {

    private SourceDestValidations() {}

    public static final List<SourceDestValidator.SourceDestValidation> PREVIEW_VALIDATIONS = Arrays.asList(
        SOURCE_MISSING_VALIDATION, REMOTE_SOURCE_VALIDATION);

    public static final List<SourceDestValidator.SourceDestValidation> ALL_VALIDATIONS = Arrays.asList(
        SOURCE_MISSING_VALIDATION,
        REMOTE_SOURCE_VALIDATION,
        DESTINATION_IN_SOURCE_VALIDATION,
        DESTINATION_SINGLE_INDEX_VALIDATION
    );

    public static final List<SourceDestValidator.SourceDestValidation> NON_DEFERABLE_VALIDATIONS = Collections.singletonList(
        DESTINATION_SINGLE_INDEX_VALIDATION
    );
}
