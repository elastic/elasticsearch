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

import static org.elasticsearch.xpack.inference.InferencePlugin.EIS_INFERENCE_FEATURE;
import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_API_FEATURE;

public class InferenceLicenceCheck {

    private InferenceLicenceCheck() {}

    public static boolean isServiceLicenced(String serviceName, XPackLicenseState licenseState) {
        if (ElasticInferenceService.NAME.equals(serviceName)) {
            return EIS_INFERENCE_FEATURE.check(licenseState);
        } else {
            return INFERENCE_API_FEATURE.check(licenseState);
        }
    }

    public static ElasticsearchSecurityException complianceException(String serviceName) {
        if (ElasticInferenceService.NAME.equals(serviceName)) {
            return LicenseUtils.newComplianceException(XPackField.ELASTIC_INFERENCE_SERVICE);
        } else {
            return LicenseUtils.newComplianceException(XPackField.INFERENCE);
        }
    }
}
