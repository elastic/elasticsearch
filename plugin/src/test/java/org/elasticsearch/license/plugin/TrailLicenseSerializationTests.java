/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.TrialLicenseUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class TrailLicenseSerializationTests extends ElasticsearchTestCase {

    @Test
    public void testSerialization() throws Exception {
        final TrialLicenseUtils.TrialLicenseBuilder trialLicenseBuilder = TrialLicenseUtils.builder()
                .duration(TimeValue.timeValueHours(2))
                .maxNodes(5)
                .issuedTo("customer")
                .issueDate(System.currentTimeMillis());
        int n = randomIntBetween(2, 5);
        List<License> trialLicenses = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            trialLicenses.add(trialLicenseBuilder.feature("feature__" + String.valueOf(i)).build());
        }
        for (License trialLicense : trialLicenses) {
            String encodedTrialLicense = TrialLicenseUtils.toEncodedTrialLicense(trialLicense);
            final License fromEncodedTrialLicense = TrialLicenseUtils.fromEncodedTrialLicense(encodedTrialLicense);
            TestUtils.isSame(fromEncodedTrialLicense, trialLicense);
        }
    }
}
