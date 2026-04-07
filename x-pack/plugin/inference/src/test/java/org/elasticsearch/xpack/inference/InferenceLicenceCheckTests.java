/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;

public class InferenceLicenceCheckTests extends ESTestCase {
    public void testIsServiceLicenced_WithElasticInferenceService_WhenLicensed() {
        var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(InferencePlugin.EIS_INFERENCE_FEATURE)).thenReturn(true);

        assertTrue(InferenceLicenceCheck.isServiceLicenced(ElasticInferenceService.NAME, licenseState));
    }

    public void testIsServiceLicenced_WithElasticInferenceService_WhenNotLicensed() {
        var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(InferencePlugin.EIS_INFERENCE_FEATURE)).thenReturn(false);

        assertFalse(InferenceLicenceCheck.isServiceLicenced(ElasticInferenceService.NAME, licenseState));
    }

    public void testIsServiceLicenced_WithOtherService_WhenLicensed() {
        var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(InferencePlugin.INFERENCE_API_FEATURE)).thenReturn(true);
        boolean result = InferenceLicenceCheck.isServiceLicenced("openai", licenseState);
        assertTrue(result);
    }

    public void testIsServiceLicenced_WithOtherService_WhenNotLicensed() {
        var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(InferencePlugin.INFERENCE_API_FEATURE)).thenReturn(false);
        boolean result = InferenceLicenceCheck.isServiceLicenced("cohere", licenseState);
        assertFalse(result);
    }

    public void testIsServiceLicenced_WithMultipleServices() {
        var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(InferencePlugin.EIS_INFERENCE_FEATURE)).thenReturn(true);
        when(licenseState.isAllowed(InferencePlugin.INFERENCE_API_FEATURE)).thenReturn(false);

        // Elastic Inference Service should be licensed
        assertTrue(InferenceLicenceCheck.isServiceLicenced(ElasticInferenceService.NAME, licenseState));

        // Other services should not be licensed
        assertFalse(InferenceLicenceCheck.isServiceLicenced("openai", licenseState));
        assertFalse(InferenceLicenceCheck.isServiceLicenced("huggingface", licenseState));
    }

    public void testComplianceException_WithElasticInferenceService() {
        ElasticsearchSecurityException exception = InferenceLicenceCheck.complianceException(ElasticInferenceService.NAME);

        assertNotNull(exception);
        assertThat(exception.getMessage(), containsString("current license is non-compliant for [Elastic Inference Service]"));
    }

    public void testComplianceException_WithOtherService() {
        ElasticsearchSecurityException exception = InferenceLicenceCheck.complianceException("openai");

        assertNotNull(exception);
        assertThat(exception.getMessage(), containsString("current license is non-compliant for [inference]"));
    }

    public void testComplianceException_WithMultipleServices() {
        // Test that different services return different exceptions
        ElasticsearchSecurityException eisException = InferenceLicenceCheck.complianceException(ElasticInferenceService.NAME);
        ElasticsearchSecurityException openaiException = InferenceLicenceCheck.complianceException("openai");
        ElasticsearchSecurityException cohereException = InferenceLicenceCheck.complianceException("cohere");

        assertThat(eisException.getMessage(), containsString("Elastic Inference Service"));
        assertThat(openaiException.getMessage(), containsString("inference"));
        assertThat(cohereException.getMessage(), containsString("inference"));

        // The two non-EIS exceptions should have the same message
        assertEquals(openaiException.getMessage(), cohereException.getMessage());
    }

    public void testComplianceException_WithNullServiceName() {
        ElasticsearchSecurityException exception = InferenceLicenceCheck.complianceException(null);

        assertNotNull(exception);
        assertThat(exception.getMessage(), containsString("current license is non-compliant for [inference]"));
    }
}
