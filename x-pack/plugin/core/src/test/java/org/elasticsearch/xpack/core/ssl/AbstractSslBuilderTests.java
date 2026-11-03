/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractSslBuilderTests extends ESTestCase {

    public void testBuildUsesConfiguredProtocolsCiphersAndVerificationMode() throws Exception {
        final SSLContext sslContext = SSLContext.getDefault();
        final List<String> ciphers = randomSubsetOf(
            randomIntBetween(1, 3),
            Arrays.asList(sslContext.getSupportedSSLParameters().getCipherSuites())
        );
        final List<String> protocols = randomSubsetOf(randomIntBetween(1, 2), List.of("TLSv1.3", "TLSv1.2"));
        final SslVerificationMode mode = randomFrom(SslVerificationMode.values());
        final SslConfiguration config = loadConfig(
            Settings.builder()
                .putList("supported_protocols", protocols)
                .putList("cipher_suites", ciphers)
                .put("verification_mode", mode.name())
                .build()
        );

        final BuildArgs args = new CapturingBuilder().build(config, sslContext);

        assertThat(args.sslContext(), sameInstance(sslContext));
        assertThat(args.protocols(), arrayContaining(protocols.toArray()));
        assertThat(args.ciphers(), arrayContaining(ciphers.toArray()));
        if (mode.isHostnameVerificationEnabled()) {
            assertThat(args.verifier(), instanceOf(DefaultHostnameVerifier.class));
        } else {
            assertThat(args.verifier(), sameInstance(NoopHostnameVerifier.INSTANCE));
        }
    }

    public void testBuildDropsUnsupportedCiphers() throws Exception {
        final SSLContext sslContext = SSLContext.getDefault();
        final List<String> supported = randomSubsetOf(
            randomIntBetween(1, 3),
            Arrays.asList(sslContext.getSupportedSSLParameters().getCipherSuites())
        );
        final List<String> requested = new ArrayList<>(supported);
        requested.add(randomIntBetween(0, requested.size()), "INVALID_CIPHER");
        final SslConfiguration config = loadConfig(Settings.builder().putList("cipher_suites", requested).build());

        final BuildArgs args = new CapturingBuilder().build(config, sslContext);

        assertThat(args.ciphers(), arrayContaining(supported.toArray()));
    }

    public void testHostnameVerifierRejectsPublicSuffixIdentities() throws Exception {
        final SslConfiguration config = loadConfig(Settings.builder().put("verification_mode", "full").build());
        final var verifier = (DefaultHostnameVerifier) new CapturingBuilder().build(config, SSLContext.getDefault()).verifier();

        verifier.verify("foo.example.com", certWithDnsSan("*.example.com"));
        expectThrows(SSLPeerUnverifiedException.class, () -> verifier.verify("co.uk", certWithDnsSan("co.uk")));
    }

    private SslConfiguration loadConfig(Settings settings) {
        return SslSettingsLoader.load(
            settings,
            null,
            TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build())
        );
    }

    /**
     * Mocked because the verifier only reads the subject alternative names, and a real certificate per case would need a CA setup.
     */
    private static X509Certificate certWithDnsSan(String dnsName) throws CertificateParsingException {
        final X509Certificate cert = mock(X509Certificate.class);
        when(cert.getSubjectAlternativeNames()).thenReturn(List.<List<?>>of(List.of(2, dnsName)));
        return cert;
    }

    private record BuildArgs(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier) {}

    private static class CapturingBuilder extends AbstractSslBuilder<BuildArgs> {
        @Override
        protected BuildArgs build(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier) {
            return new BuildArgs(sslContext, protocols, ciphers, verifier);
        }
    }
}
