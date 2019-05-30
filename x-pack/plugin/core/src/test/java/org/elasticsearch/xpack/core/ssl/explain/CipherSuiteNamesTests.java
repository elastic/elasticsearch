/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class CipherSuiteNamesTests extends ESTestCase {

    public void testNameForCipher() {
        assertThat(CipherSuiteNames.nameFor((byte) 0x00, (byte) 0x03), Matchers.equalTo("TLS_RSA_EXPORT_WITH_RC4_40_MD5"));
        assertThat(CipherSuiteNames.nameFor((byte) 0x13, (byte) 0x03), Matchers.equalTo("TLS_CHACHA20_POLY1305_SHA256"));
        assertThat(CipherSuiteNames.nameFor((byte) 0xC0, (byte) 0x03), Matchers.equalTo("TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA"));
        assertThat(CipherSuiteNames.nameFor((byte) 0xD0, (byte) 0x03), Matchers.equalTo("TLS_ECDHE_PSK_WITH_AES_128_CCM_8_SHA256"));
        assertThat(CipherSuiteNames.nameFor((byte) 0x03, (byte) 0x03), Matchers.equalTo("Unknown_Cipher(0x03,0x03)"));

        assertThat(CipherSuiteNames.nameFor((byte) 0x00, (byte) 0x10), Matchers.equalTo("TLS_DH_RSA_WITH_3DES_EDE_CBC_SHA"));
        assertThat(CipherSuiteNames.nameFor((byte) 0xC0, (byte) 0x10), Matchers.equalTo("TLS_ECDHE_RSA_WITH_NULL_SHA"));
        assertThat(CipherSuiteNames.nameFor((byte) 0x0C, (byte) 0x10), Matchers.equalTo("Unknown_Cipher(0x0C,0x10)"));
        assertThat(CipherSuiteNames.nameFor((byte) 0xCC, (byte) 0x10), Matchers.equalTo("Unknown_Cipher(0xCC,0x10)"));

        assertThat(CipherSuiteNames.nameFor((byte) 0x00, (byte) 0xA9), Matchers.equalTo("TLS_PSK_WITH_AES_256_GCM_SHA384"));
        assertThat(CipherSuiteNames.nameFor((byte) 0xC0, (byte) 0xA9), Matchers.equalTo("TLS_PSK_WITH_AES_256_CCM_8"));
        assertThat(CipherSuiteNames.nameFor((byte) 0xCC, (byte) 0xA9), Matchers.equalTo("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"));
        assertThat(CipherSuiteNames.nameFor((byte) 0x22, (byte) 0xA9), Matchers.equalTo("Unknown_Cipher(0x22,0xA9)"));
    }


}
