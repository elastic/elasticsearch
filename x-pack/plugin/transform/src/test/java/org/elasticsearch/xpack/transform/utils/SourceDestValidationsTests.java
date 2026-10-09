/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SourceDestValidation;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.List;

import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_IN_SOURCE_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_PIPELINE_MISSING_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_SINGLE_INDEX_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SOURCE_MISSING_VALIDATION;
import static org.elasticsearch.xpack.transform.utils.SourceDestValidations.REMOTE_SOURCE_VALIDATION;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SourceDestValidationsTests extends ESTestCase {

    private static CrossProjectModeDecider cpsEnabled() {
        var d = mock(CrossProjectModeDecider.class);
        when(d.crossProjectEnabled()).thenReturn(true);
        return d;
    }

    private static CrossProjectModeDecider cpsDisabled() {
        var d = mock(CrossProjectModeDecider.class);
        when(d.crossProjectEnabled()).thenReturn(false);
        return d;
    }

    public void testGetValidations_CpsDisabled_IncludesSourceMissingAndRemoteSource() {
        assertThat(
            SourceDestValidations.getValidations(false, cpsDisabled(), List.of()),
            containsInAnyOrder(
                SOURCE_MISSING_VALIDATION,
                DESTINATION_IN_SOURCE_VALIDATION,
                DESTINATION_SINGLE_INDEX_VALIDATION,
                DESTINATION_PIPELINE_MISSING_VALIDATION,
                REMOTE_SOURCE_VALIDATION
            )
        );
    }

    public void testGetValidations_CpsEnabled_SkipsSourceMissingRemoteSourceAndDestinationInSource() {
        assumeTrue("Only relevant when CPS feature flag is on", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        assertThat(
            SourceDestValidations.getValidations(false, cpsEnabled(), List.of()),
            containsInAnyOrder(DESTINATION_SINGLE_INDEX_VALIDATION, DESTINATION_PIPELINE_MISSING_VALIDATION)
        );
    }

    public void testGetValidations_DeferValidation_AlwaysReturnsOnlySingleIndex() {
        assertThat(
            SourceDestValidations.getValidations(true, cpsDisabled(), List.of()),
            containsInAnyOrder(DESTINATION_SINGLE_INDEX_VALIDATION)
        );

        assumeTrue("Only relevant when CPS feature flag is on", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        assertThat(
            SourceDestValidations.getValidations(true, cpsEnabled(), List.of()),
            containsInAnyOrder(DESTINATION_SINGLE_INDEX_VALIDATION)
        );
    }

    public void testGetValidationsForPreview_CpsDisabled_IncludesSourceMissingAndRemoteSource() {
        assertThat(
            SourceDestValidations.getValidationsForPreview(cpsDisabled(), List.of()),
            containsInAnyOrder(SOURCE_MISSING_VALIDATION, DESTINATION_PIPELINE_MISSING_VALIDATION, REMOTE_SOURCE_VALIDATION)
        );
    }

    public void testGetValidationsForPreview_CpsEnabled_SkipsSourceMissingAndRemoteSource() {
        assumeTrue("Only relevant when CPS feature flag is on", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        assertThat(
            SourceDestValidations.getValidationsForPreview(cpsEnabled(), List.of()),
            containsInAnyOrder(DESTINATION_PIPELINE_MISSING_VALIDATION)
        );
    }

    public void testGetValidations_CpsEnabled_AdditionalValidationsAppended() {
        assumeTrue("Only relevant when CPS feature flag is on", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        SourceDestValidation extra = mock(SourceDestValidator.SourceDestValidation.class);
        assertThat(
            SourceDestValidations.getValidations(false, cpsEnabled(), List.of(extra)),
            containsInAnyOrder(DESTINATION_SINGLE_INDEX_VALIDATION, DESTINATION_PIPELINE_MISSING_VALIDATION, extra)
        );
    }
}
