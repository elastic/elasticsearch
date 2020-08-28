/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import javax.net.ssl.X509ExtendedTrustManager;
import java.security.cert.X509Certificate;
import java.util.Locale;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

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
        assertHasTrustedIssuer(trustManager, "VeriSign");
        assertHasTrustedIssuer(trustManager, "GeoTrust");
        assertHasTrustedIssuer(trustManager, "DigiCert");
        assertHasTrustedIssuer(trustManager, "thawte");
        assertHasTrustedIssuer(trustManager, "COMODO");
    }

    private void assertHasTrustedIssuer(X509ExtendedTrustManager trustManager, String name) {
        final String lowerName = name.toLowerCase(Locale.ROOT);
        final Optional<X509Certificate> ca = Stream.of(trustManager.getAcceptedIssuers())
            .filter(cert -> cert.getSubjectDN().getName().toLowerCase(Locale.ROOT).contains(lowerName))
            .findAny();
        if (ca.isPresent() == false) {
            logger.info("Failed to find issuer [{}] in trust manager, but did find ...", lowerName);
            for (X509Certificate cert : trustManager.getAcceptedIssuers()) {
                logger.info(" - {}", cert.getSubjectDN().getName().replaceFirst("^\\w+=([^,]+),.*", "$1"));
            }
            Assert.fail("Cannot find trusted issuer with name [" + name + "].");
        }
    }

}
