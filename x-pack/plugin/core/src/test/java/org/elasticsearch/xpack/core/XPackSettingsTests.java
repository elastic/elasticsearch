/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.test.ESTestCase;
import javax.crypto.Cipher;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class XPackSettingsTests extends ESTestCase {

    public void testDefaultSSLCiphers() throws Exception {
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_RSA_WITH_AES_128_CBC_SHA"));

        final boolean useAES256 = Cipher.getMaxAllowedKeyLength("AES") > 128;
        if (useAES256) {
            logger.info("AES 256 is available");
            assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_RSA_WITH_AES_256_CBC_SHA"));
        } else {
            logger.info("AES 256 is not available");
            assertThat(XPackSettings.DEFAULT_CIPHERS, not(hasItem("TLS_RSA_WITH_AES_256_CBC_SHA")));
        }
    }
}
