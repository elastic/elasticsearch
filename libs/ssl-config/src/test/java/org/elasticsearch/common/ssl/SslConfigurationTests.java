/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.hamcrest.Matchers;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.net.ssl.SSLContext;

import static org.elasticsearch.common.ssl.SslConfigurationLoader.DEFAULT_CIPHERS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SslConfigurationTests extends ESTestCase {

    static final String[] VALID_PROTOCOLS = { "TLSv1.2", "TLSv1.1", "TLSv1", "SSLv3", "SSLv2Hello", "SSLv2" };

    public void testBasicConstruction() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        Mockito.when(trustConfig.toString()).thenReturn("TEST-TRUST");
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);
        Mockito.when(keyConfig.toString()).thenReturn("TEST-KEY");
        final SslVerificationMode verificationMode = randomFrom(SslVerificationMode.values());
        final SslClientAuthenticationMode clientAuth = randomFrom(SslClientAuthenticationMode.values());
        final List<String> ciphers = randomSubsetOf(randomIntBetween(1, DEFAULT_CIPHERS.size()), DEFAULT_CIPHERS);
        final List<String> protocols = randomSubsetOf(randomIntBetween(1, 4), VALID_PROTOCOLS);
        final SslConfiguration configuration = new SslConfiguration(
            "test.ssl",
            true,
            trustConfig,
            keyConfig,
            verificationMode,
            clientAuth,
            ciphers,
            protocols
        );

        assertThat(configuration.trustConfig(), is(trustConfig));
        assertThat(configuration.keyConfig(), is(keyConfig));
        assertThat(configuration.verificationMode(), is(verificationMode));
        assertThat(configuration.clientAuth(), is(clientAuth));
        assertThat(configuration.getCipherSuites(), is(ciphers));
        assertThat(configuration.supportedProtocols(), is(protocols));

        assertThat(configuration.toString(), containsString("TEST-TRUST"));
        assertThat(configuration.toString(), containsString("TEST-KEY"));
        assertThat(configuration.toString(), containsString(verificationMode.toString()));
        assertThat(configuration.toString(), containsString(clientAuth.toString()));
        assertThat(configuration.toString(), containsString(randomFrom(ciphers)));
        assertThat(configuration.toString(), containsString(randomFrom(protocols)));
    }

    public void testEqualsAndHashCode() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);
        final SslVerificationMode verificationMode = randomFrom(SslVerificationMode.values());
        final SslClientAuthenticationMode clientAuth = randomFrom(SslClientAuthenticationMode.values());
        final List<String> ciphers = randomSubsetOf(randomIntBetween(1, DEFAULT_CIPHERS.size() - 1), DEFAULT_CIPHERS);
        final List<String> protocols = randomSubsetOf(randomIntBetween(1, VALID_PROTOCOLS.length - 1), VALID_PROTOCOLS);
        final SslConfiguration configuration = new SslConfiguration(
            "test.ssl",
            true,
            trustConfig,
            keyConfig,
            verificationMode,
            clientAuth,
            ciphers,
            protocols
        );

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            configuration,
            orig -> new SslConfiguration(
                "test.ssl",
                true,
                orig.trustConfig(),
                orig.keyConfig(),
                orig.verificationMode(),
                orig.clientAuth(),
                orig.getCipherSuites(),
                orig.supportedProtocols()
            ),
            this::mutateSslConfiguration
        );
    }

    private SslConfiguration mutateSslConfiguration(SslConfiguration orig) {
        return switch (randomIntBetween(1, 4)) {
            case 1 -> new SslConfiguration(
                "test.ssl",
                true,
                orig.trustConfig(),
                orig.keyConfig(),
                randomValueOtherThan(orig.verificationMode(), () -> randomFrom(SslVerificationMode.values())),
                orig.clientAuth(),
                orig.getCipherSuites(),
                orig.supportedProtocols()
            );
            case 2 -> new SslConfiguration(
                "test.ssl",
                true,
                orig.trustConfig(),
                orig.keyConfig(),
                orig.verificationMode(),
                randomValueOtherThan(orig.clientAuth(), () -> randomFrom(SslClientAuthenticationMode.values())),
                orig.getCipherSuites(),
                orig.supportedProtocols()
            );
            case 3 -> new SslConfiguration(
                "test.ssl",
                true,
                orig.trustConfig(),
                orig.keyConfig(),
                orig.verificationMode(),
                orig.clientAuth(),
                DEFAULT_CIPHERS,
                orig.supportedProtocols()
            );
            default -> new SslConfiguration(
                "test.ssl",
                true,
                orig.trustConfig(),
                orig.keyConfig(),
                orig.verificationMode(),
                orig.clientAuth(),
                orig.getCipherSuites(),
                Arrays.asList(VALID_PROTOCOLS)
            );
        };
    }

    public void testDependentFiles() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);
        final SslConfiguration configuration = new SslConfiguration(
            "test.ssl",
            true,
            trustConfig,
            keyConfig,
            randomFrom(SslVerificationMode.values()),
            randomFrom(SslClientAuthenticationMode.values()),
            DEFAULT_CIPHERS,
            SslConfigurationLoader.DEFAULT_PROTOCOLS
        );

        final Path dir = createTempDir();
        final Path file1 = dir.resolve(randomAlphaOfLength(1) + ".pem");
        final Path file2 = dir.resolve(randomAlphaOfLength(2) + ".pem");
        final Path file3 = dir.resolve(randomAlphaOfLength(3) + ".pem");
        final Path file4 = dir.resolve(randomAlphaOfLength(4) + ".pem");
        final Path file5 = dir.resolve(randomAlphaOfLength(5) + ".pem");

        Mockito.when(trustConfig.getDependentFiles()).thenReturn(Arrays.asList(file1, file2));
        Mockito.when(keyConfig.getDependentFiles()).thenReturn(Arrays.asList(file3, file4, file5));
        assertThat(configuration.getDependentFiles(), Matchers.containsInAnyOrder(file1, file2, file3, file4, file5));
    }

    public void testBuildSslContext() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);
        final String protocol = randomFrom(SslConfigurationLoader.DEFAULT_PROTOCOLS);
        final SslConfiguration configuration = new SslConfiguration(
            "test.ssl",
            true,
            trustConfig,
            keyConfig,
            randomFrom(SslVerificationMode.values()),
            randomFrom(SslClientAuthenticationMode.values()),
            DEFAULT_CIPHERS,
            Collections.singletonList(protocol)
        );

        Mockito.when(trustConfig.createTrustManager()).thenReturn(null);
        Mockito.when(keyConfig.createKeyManager()).thenReturn(null);
        final SSLContext sslContext = configuration.createSslContext();
        assertThat(sslContext.getProtocol(), equalTo(protocol));

        Mockito.verify(trustConfig).createTrustManager();
        Mockito.verify(keyConfig).createKeyManager();
        Mockito.verifyNoMoreInteractions(trustConfig, keyConfig);
    }

}
