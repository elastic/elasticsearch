/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Collections;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class KeyStoreUtilTests extends ESTestCase {
    private static final char[] P12_PASS = "p12-pass".toCharArray();

    public void testFilter() throws Exception {
        assumeFalse("Can't use PKCS#12 keystores in a FIPS JVM", inFipsJvm());

        final Path p12 = getDataPath("/certs/cert-all/certs.p12");
        final KeyStore original = KeyStoreUtil.readKeyStore(p12, "PKCS12", P12_PASS);

        // No-op filter
        final KeyStore clone = KeyStoreUtil.filter(KeyStoreUtil.readKeyStore(p12, "PKCS12", P12_PASS), entry -> true);
        assertThat(Collections.list(clone.aliases()), containsInAnyOrder("cert1", "cert2"));
        assertSameEntry(original, clone, "cert1", P12_PASS);
        assertSameEntry(original, clone, "cert2", P12_PASS);

        // Filter by alias
        final KeyStore cert1 = KeyStoreUtil.filter(
            KeyStoreUtil.readKeyStore(p12, "PKCS12", P12_PASS),
            entry -> entry.getAlias().equals("cert1")
        );
        assertThat(Collections.list(cert1.aliases()), containsInAnyOrder("cert1"));
        assertSameEntry(original, cert1, "cert1", P12_PASS);

        // Filter by cert
        final KeyStore cert2 = KeyStoreUtil.filter(
            KeyStoreUtil.readKeyStore(p12, "PKCS12", P12_PASS),
            entry -> entry.getX509Certificate().getSubjectX500Principal().getName().equals("CN=cert2")
        );
        assertThat(Collections.list(cert2.aliases()), containsInAnyOrder("cert2"));
        assertSameEntry(original, cert2, "cert2", P12_PASS);
    }

    private void assertSameEntry(KeyStore ks1, KeyStore ks2, String alias, char[] keyPassword) throws Exception {
        assertThat(ks1.isKeyEntry(alias), equalTo(ks2.isKeyEntry(alias)));
        assertThat(ks1.isCertificateEntry(alias), equalTo(ks2.isCertificateEntry(alias)));
        assertThat(ks1.getCertificate(alias), equalTo(ks2.getCertificate(alias)));
        assertThat(ks1.getCertificateChain(alias), equalTo(ks2.getCertificateChain(alias)));
        assertThat(ks1.getKey(alias, P12_PASS), equalTo(ks2.getKey(alias, keyPassword)));
    }
}
