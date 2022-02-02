/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class LicenseStatusTests extends ESTestCase {
    public void testSerialization() throws IOException {
        LicenseStatus status = randomFrom(LicenseStatus.values());
        assertSame(status, copyWriteable(status, writableRegistry(), LicenseStatus::readFrom));
    }

}
