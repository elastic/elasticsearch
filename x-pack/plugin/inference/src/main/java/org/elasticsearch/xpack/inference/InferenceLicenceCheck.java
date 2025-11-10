/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_API_FEATURE;

public class InferenceLicenceCheck {

    private InferenceLicenceCheck() {}

    /**
     * Is the Inference service compliant with the current license.
     * @param serviceName The Inference service name
     * @param licenseState The current license state
     * @return True if the licence is sufficient
     */
    public static boolean isServiceLicenced(String serviceName, XPackLicenseState licenseState) {
        // EIS is always available.
        return ElasticInferenceService.NAME.equals(serviceName) || INFERENCE_API_FEATURE.check(licenseState);
    }

    public static ElasticsearchSecurityException complianceException(String serviceName) {
        if (ElasticInferenceService.NAME.equals(serviceName)) {
            return LicenseUtils.newComplianceException(XPackField.ELASTIC_INFERENCE_SERVICE);
        } else {
            return LicenseUtils.newComplianceException(XPackField.INFERENCE);
        }
    }
}
