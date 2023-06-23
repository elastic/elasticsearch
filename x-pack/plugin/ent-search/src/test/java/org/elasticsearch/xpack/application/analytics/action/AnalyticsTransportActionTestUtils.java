/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AnalyticsTransportActionTestUtils {

    /**
     * Creates a mock license state object with the specified license support status.
     *
     * @param supported a boolean value indicating whether the license is supported
     *
     * @return a {@link MockLicenseState} object with the specified license support status
     */
    public static MockLicenseState mockLicenseState(boolean supported) {
        MockLicenseState licenseState = mock(MockLicenseState.class);

        when(licenseState.isAllowed(LicenseUtils.LICENSED_ENT_SEARCH_FEATURE)).thenReturn(supported);
        when(licenseState.isActive()).thenReturn(supported);
        when(licenseState.statusDescription()).thenReturn("invalid license");

        return licenseState;
    }

    /**
     * Verifies that an ElasticsearchSecurityException is thrown with a specific message when an invalid license is used.
     *
     * @param listener an {@link ActionListener} the exception should be sent through.
     */
    public static void verifyExceptionIsThrownOnInvalidLicence(ActionListener<?> listener) {
        verify(listener, times(1)).onFailure(argThat((ElasticsearchSecurityException e) -> {
            ESTestCase.assertTrue(e.getMessage().contains("require an active trial, platinum or enterprise license"));
            return true;
        }));
    }

    /**
     * Verifies that no exception is thrown by trhough specified listener object.
     *
     * @param listener an {@link ActionListener} object to verify
     */
    public static void verifyNoExceptionIsThrown(ActionListener<?> listener) {
        verify(listener, never()).onFailure((any()));
    }

    /**
     * Verifies that no response is sent to the specified listener object.
     * @param listener an {@link ActionListener} object to verify
     */
    public static void verifyNoResponseIsSent(ActionListener<?> listener) {
        verify(listener, never()).onResponse((any()));
    }
}
