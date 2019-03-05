/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;


import org.elasticsearch.test.ESTestCase;

import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.hamcrest.Matchers.startsWith;

public class LicenseServiceTests extends ESTestCase {

    public void testLogExpirationWarning() {
        long time = LocalDate.of(2018, 11, 15).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        final boolean expired = randomBoolean();
        final String message = LicenseService.buildExpirationMessage(time, expired).toString();
        if (expired) {
            assertThat(message, startsWith("LICENSE [EXPIRED] ON [THU, NOV 15, 2018].\n"));
        } else {
            assertThat(message, startsWith("License [will expire] on [Thu, Nov 15, 2018].\n"));
        }
    }

}
