/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.license;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class LicenseStatusTests extends ESTestCase {

    public void testCompatibility() {
        final LicenseStatus[] values = LicenseStatus.values();
        final LicenseStatus[] hlrcValues = LicenseStatus.values();

        assertThat(values.length, equalTo(hlrcValues.length));

        for (LicenseStatus value : values) {
            final LicenseStatus licenseStatus = LicenseStatus.fromString(value.label());
            assertThat(licenseStatus.label(), equalTo(value.label()));
        }
    }
}
