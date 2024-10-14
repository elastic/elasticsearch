/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.test.ESTestCase;

public class ElasticInferenceServiceSettingsTests extends ESTestCase {

    public void testEisGatewayURLValidator_Validate_ThrowError_OnMissingURIScheme() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ElasticInferenceServiceSettings.EisGatewayURLValidator().validate("www.missing-scheme-gateway-url.com")
        );
    }

    public void testEisGatewayURLValidator_Validate_ThrowError_OnWrongURIScheme() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ElasticInferenceServiceSettings.EisGatewayURLValidator().validate("file://www.missing-scheme-gateway-url.com")
        );
    }

    public void testEisGatewayURLValidator_Validate_DoesNotThrowError_ForHTTP() {
        var scheme = "http";

        try {
            new ElasticInferenceServiceSettings.EisGatewayURLValidator().validate(scheme + "://www.valid-gateway-url.com");
        } catch (Exception e) {
            fail(e, "Should not throw exception for " + "[" + scheme + "]");
        }
    }

    public void testEisGatewayURLValidator_Validate_DoesNotThrowError_ForHTTPS() {
        var scheme = "https";

        try {
            new ElasticInferenceServiceSettings.EisGatewayURLValidator().validate(scheme + "://www.valid-gateway-url.com");
        } catch (Exception e) {
            fail(e, "Should not throw exception for " + "[" + scheme + "]");
        }
    }

    public void testEisGatewayURLValidator_Validate_DoesNotThrowError_IfURLNull() {
        try {
            new ElasticInferenceServiceSettings.EisGatewayURLValidator().validate(null);
        } catch (Exception e) {
            fail(e, "Should not throw exception for, if eis-gateway URL is null");
        }
    }

    public void testEisGatewayURLValidator_Validate_DoesNotThrowError_IfURLEmpty() {
        try {
            new ElasticInferenceServiceSettings.EisGatewayURLValidator().validate("");
        } catch (Exception e) {
            fail(e, "Should not throw exception for, if eis-gateway URL is empty");
        }
    }

}
