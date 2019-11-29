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

/**
 * Due to changes in JDK9 where locale data is used from CLDR, the licence message will differ in jdk 8 and jdk9+
 * https://openjdk.java.net/jeps/252
 * We run ES with -Djava.locale.providers=SPI,COMPAT and same option has to be applied when running this test from IDE
 */
public class LicenseServiceTests extends ESTestCase {

    public void testLogExpirationWarning() {
        long time = LocalDate.of(2018, 11, 15).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        final boolean expired = randomBoolean();
        final String message = LicenseService.buildExpirationMessage(time, expired).toString();
        if (expired) {
            assertThat(message, startsWith("LICENSE [EXPIRED] ON [THURSDAY, NOVEMBER 15, 2018].\n"));
        } else {
            assertThat(message, startsWith("License [will expire] on [Thursday, November 15, 2018].\n"));
        }
    }
}
