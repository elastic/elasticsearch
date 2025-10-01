/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SSLIOSessionStrategyBuilderTests extends ESTestCase {

    public void testBuildSSLStrategy() {
        var env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        // this just exhaustively verifies that the right things are called and that it uses the right parameters
        SslVerificationMode mode = randomFrom(SslVerificationMode.values());
        Settings settings = Settings.builder()
            .put("supported_protocols", "protocols")
            .put("cipher_suites", "INVALID_CIPHER")
            .put("verification_mode", mode.name())
            .build();
        SSLIOSessionStrategyBuilder builder = mock(SSLIOSessionStrategyBuilder.class);
        SslConfiguration sslConfig = SslSettingsLoader.load(settings, null, env);
        SSLParameters sslParameters = mock(SSLParameters.class);
        SSLContext sslContext = mock(SSLContext.class);
        String[] protocols = new String[] { "protocols" };
        String[] ciphers = new String[] { "ciphers!!!" };
        String[] supportedCiphers = new String[] { "supported ciphers" };
        List<String> requestedCiphers = List.of("INVALID_CIPHER");
        SSLIOSessionStrategy sslStrategy = mock(SSLIOSessionStrategy.class);

        when(builder.supportedCiphers(any(String[].class), anyList(), any(Boolean.TYPE))).thenAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args[0], is(supportedCiphers));
            assertThat(args[1], is(requestedCiphers));
            assertThat(args[2], is(false));
            return ciphers;
        });
        when(builder.sslParameters(sslContext)).thenReturn(sslParameters);
        when(sslParameters.getCipherSuites()).thenReturn(supportedCiphers);

        when(builder.build(any(SSLContext.class), any(String[].class), any(String[].class), any(HostnameVerifier.class))).thenAnswer(
            inv -> {
                final Object[] args = inv.getArguments();
                assertThat(args[0], is(sslContext));
                assertThat(args[1], is(protocols));
                assertThat(args[2], is(ciphers));
                if (mode.isHostnameVerificationEnabled()) {
                    assertThat(args[3], instanceOf(DefaultHostnameVerifier.class));
                } else {
                    assertThat(args[3], sameInstance(NoopHostnameVerifier.INSTANCE));
                }
                return sslStrategy;
            }
        );

        when(builder.build(Mockito.eq(sslConfig), Mockito.any(SSLContext.class))).thenCallRealMethod();

        final SslConfiguration config = new SSLService(env).sslConfiguration(settings);
        final SSLIOSessionStrategy actual = builder.build(config, sslContext);
        assertThat(actual, sameInstance(sslStrategy));
    }

}
