/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class LicenseStatusTests extends ESTestCase {
    public void testCompatibility() {
        final LicenseStatus[] values = LicenseStatus.values();
        final org.elasticsearch.client.license.LicenseStatus[] ossValues =
            org.elasticsearch.client.license.LicenseStatus.values();

        assertThat(values.length, equalTo(ossValues.length));

        for (LicenseStatus value : values) {
            final org.elasticsearch.client.license.LicenseStatus licenseStatus =
                org.elasticsearch.client.license.LicenseStatus.fromString(value.label());
            assertThat(licenseStatus.label(), equalTo(value.label()));
        }
    }
}
