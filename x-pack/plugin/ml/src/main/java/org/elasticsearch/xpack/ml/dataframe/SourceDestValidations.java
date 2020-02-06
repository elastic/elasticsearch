/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_IN_SOURCE_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_SINGLE_INDEX_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.REMOTE_SOURCE_NOT_SUPPORTED_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SOURCE_MISSING_VALIDATION;

public final class SourceDestValidations {

    private SourceDestValidations() {}

    public static final List<SourceDestValidator.SourceDestValidation> ALL_VALIDATIONS = Arrays.asList(
        SOURCE_MISSING_VALIDATION,
        DESTINATION_SINGLE_INDEX_VALIDATION,
        DESTINATION_IN_SOURCE_VALIDATION,
        REMOTE_SOURCE_NOT_SUPPORTED_VALIDATION
    );
}
