/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.ssl.DefaultJdkTrustConfig;
import org.elasticsearch.common.ssl.EmptyKeyConfig;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteConnectionManager;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractServerTransportInterceptorTests extends ESTestCase {

    protected static String[] randomRoles() {
        return generateRandomStringArray(3, 10, false, true);
    }

    protected
        Function<Transport.Connection, Optional<RemoteConnectionManager.RemoteClusterAliasWithCredentials>>
        mockRemoteClusterCredentialsResolver(String remoteClusterAlias) {
        return connection -> Optional.of(
            new RemoteConnectionManager.RemoteClusterAliasWithCredentials(
                remoteClusterAlias,
                new SecureString(randomAlphaOfLengthBetween(10, 42).toCharArray())
            )
        );
    }

    protected static SSLService mockSslService() {
        final SslConfiguration defaultConfiguration = new SslConfiguration(
            "",
            false,
            DefaultJdkTrustConfig.DEFAULT_INSTANCE,
            EmptyKeyConfig.INSTANCE,
            SslVerificationMode.FULL,
            SslClientAuthenticationMode.NONE,
            List.of("TLS_AES_256_GCM_SHA384"),
            List.of("TLSv1.3"),
            randomLongBetween(1, 100000)
        );
        final SslProfile defaultProfile = mock(SslProfile.class);
        when(defaultProfile.configuration()).thenReturn(defaultConfiguration);
        final SSLService sslService = mock(SSLService.class);
        when(sslService.profile("xpack.security.transport.ssl")).thenReturn(defaultProfile);
        when(sslService.profile("xpack.security.transport.ssl.")).thenReturn(defaultProfile);
        return sslService;
    }

    @SuppressWarnings("unchecked")
    protected static Consumer<ThreadContext.StoredContext> anyConsumer() {
        return any(Consumer.class);
    }
}
