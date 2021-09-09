/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.elasticsearch.test.ESTestCase;

import java.security.Security;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class Fips140ProviderVerificationTests extends ESTestCase {

    public void testBcFipsProviderInUse() {
        if (inFipsJvm()) {
            assertThat(Security.getProviders().length > 0, equalTo(true));
            assertThat(Security.getProviders()[0].getName(), containsString("BCFIPS"));
        }
    }

    public void testInApprovedOnlyMode() {
        if (inFipsJvm()) {
            assertThat(CryptoServicesRegistrar.isInApprovedOnlyMode(), equalTo(true));
        }
    }

}
