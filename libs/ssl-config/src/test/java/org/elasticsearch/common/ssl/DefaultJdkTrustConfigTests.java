/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.security.cert.X509Certificate;
import java.util.Locale;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import javax.net.ssl.X509ExtendedTrustManager;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.not;

public class DefaultJdkTrustConfigTests extends ESTestCase {

    private static final BiFunction<String, String, String> EMPTY_SYSTEM_PROPERTIES = (key, defaultValue) -> defaultValue;

    public void testGetSystemTrustStoreWithNoSystemProperties() throws Exception {
        final DefaultJdkTrustConfig trustConfig = new DefaultJdkTrustConfig((key, defaultValue) -> defaultValue);
        assertThat(trustConfig.getDependentFiles(), emptyIterable());
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        assertStandardIssuers(trustManager);
    }

    public void testGetNonPKCS11TrustStoreWithPasswordSet() throws Exception {
        final DefaultJdkTrustConfig trustConfig = new DefaultJdkTrustConfig(EMPTY_SYSTEM_PROPERTIES, "fakepassword".toCharArray());
        assertThat(trustConfig.getDependentFiles(), emptyIterable());
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        assertStandardIssuers(trustManager);
    }

    private void assertStandardIssuers(X509ExtendedTrustManager trustManager) {
        assertThat(trustManager.getAcceptedIssuers(), not(emptyArray()));
        // This is a sample of the CAs that we expect on every JRE.
        // We can safely change this list if the JRE's issuer list changes, but we want to assert something useful.
        assertHasTrustedIssuer(trustManager, "DigiCert");
        assertHasTrustedIssuer(trustManager, "COMODO");
        assertHasTrustedIssuer(trustManager, "GlobalSign");
        assertHasTrustedIssuer(trustManager, "GoDaddy");
        assertHasTrustedIssuer(trustManager, "QuoVadis");
        assertHasTrustedIssuer(trustManager, "Internet Security Research Group");
    }

    private void assertHasTrustedIssuer(X509ExtendedTrustManager trustManager, String name) {
        final String lowerName = name.toLowerCase(Locale.ROOT);
        final Optional<X509Certificate> ca = Stream.of(trustManager.getAcceptedIssuers())
            .filter(cert -> cert.getSubjectX500Principal().getName().toLowerCase(Locale.ROOT).contains(lowerName))
            .findAny();
        if (ca.isPresent() == false) {
            logger.info("Failed to find issuer [{}] in trust manager, but did find ...", lowerName);
            for (X509Certificate cert : trustManager.getAcceptedIssuers()) {
                logger.info(" - {}", cert.getSubjectX500Principal().getName().replaceFirst("^\\w+=([^,]+),.*", "$1"));
            }
            Assert.fail("Cannot find trusted issuer with name [" + name + "].");
        }
    }

}
