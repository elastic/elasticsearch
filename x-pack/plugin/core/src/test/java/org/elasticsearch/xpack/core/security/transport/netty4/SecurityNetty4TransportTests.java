/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.transport.netty4;

import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TLSv1DeprecationHandler;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.hamcrest.Matchers;

import javax.net.ssl.SSLSession;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport.getTransportProfileConfigurations;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityNetty4TransportTests extends ESTestCase {

    public void testGetSecureTransportProfileConfigurations() {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.verification_mode", VerificationMode.CERTIFICATE.name())
            .put("transport.profiles.full.xpack.security.ssl.verification_mode", VerificationMode.FULL.name())
            .put("transport.profiles.cert.xpack.security.ssl.verification_mode", VerificationMode.CERTIFICATE.name())
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration defaultConfig = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        final Map<String, SSLConfiguration> profileConfigurations = getTransportProfileConfigurations(settings, sslService, defaultConfig);
        assertThat(profileConfigurations.size(), Matchers.equalTo(3));
        assertThat(profileConfigurations.keySet(), Matchers.containsInAnyOrder("full", "cert", "default"));
        assertThat(profileConfigurations.get("full").verificationMode(), Matchers.equalTo(VerificationMode.FULL));
        assertThat(profileConfigurations.get("cert").verificationMode(), Matchers.equalTo(VerificationMode.CERTIFICATE));
        assertThat(profileConfigurations.get("default"), Matchers.sameInstance(defaultConfig));
    }

    public void testGetInsecureTransportProfileConfigurations() {
        assumeFalse("Can't run in a FIPS JVM with verification mode None", inFipsJvm());
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.verification_mode", VerificationMode.CERTIFICATE.name())
            .put("transport.profiles.none.xpack.security.ssl.verification_mode", VerificationMode.NONE.name())
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration defaultConfig = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        final Map<String, SSLConfiguration> profileConfigurations = getTransportProfileConfigurations(settings, sslService, defaultConfig);
        assertThat(profileConfigurations.size(), Matchers.equalTo(2));
        assertThat(profileConfigurations.keySet(), Matchers.containsInAnyOrder("none", "default"));
        assertThat(profileConfigurations.get("none").verificationMode(), Matchers.equalTo(VerificationMode.NONE));
        assertThat(profileConfigurations.get("default"), Matchers.sameInstance(defaultConfig));
    }

    public void testBuildTlsDeprecationHandlers() {
        final Settings settings = Settings.builder()
            .putList("transport.profiles.a.xpack.security.ssl.supported_protocols", "TLSv1.2")
            .put("transport.profiles.b.xpack.security.ssl.enabled", true)
            .put("transport.profiles.c.port", 9393)
            .build();
        final Set<String> profiles = Sets.newHashSet(TransportSettings.DEFAULT_PROFILE, "a", "b", "c");
        final Map<String, TLSv1DeprecationHandler> handlers = SecurityNetty4Transport.buildTlsDeprecationHandlers(settings, profiles);

        assertThat(handlers.keySet(), containsInAnyOrder(TransportSettings.DEFAULT_PROFILE, "a", "b", "c"));
        assertThat(handlers.get(TransportSettings.DEFAULT_PROFILE).shouldLogWarnings(), is(true));
        assertThat(handlers.get("a").shouldLogWarnings(), is(false));
        assertThat(handlers.get("b").shouldLogWarnings(), is(true));
        assertThat(handlers.get("c").shouldLogWarnings(), is(true));

        assertTlsDeprecationWarning(handlers.get(TransportSettings.DEFAULT_PROFILE), true, "xpack.security.transport.ssl.");
        assertTlsDeprecationWarning(handlers.get("a"), false, "transport.profiles.a.xpack.security.ssl.");
        assertTlsDeprecationWarning(handlers.get("b"), true, "transport.profiles.b.xpack.security.ssl.");
        assertTlsDeprecationWarning(handlers.get("c"), true, "transport.profiles.c.xpack.security.ssl.");
    }

    private void assertTlsDeprecationWarning(TLSv1DeprecationHandler handler, boolean expectWarning, String settingPrefix) {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        DeprecationLogger.setThreadContext(threadContext);
        try {
            final SSLSession sslSession = mock(SSLSession.class);

            when(sslSession.getProtocol()).thenReturn(randomFrom("TLSv1.1", "TLSv1.2"));
            handler.checkAndLog(sslSession, () -> "No warning expected");
            assertThat(threadContext.getResponseHeaders(), not(hasKey("Warning")));

            when(sslSession.getProtocol()).thenReturn("TLSv1");
            handler.checkAndLog(sslSession, () -> "Test TLSv1");
            if (expectWarning) {
                assertThat(threadContext.getResponseHeaders(), hasKey("Warning"));

                assertWarnings("a TLS v1.0 session was used for [Test TLSv1]," +
                    " this protocol will be disabled by default in a future version." +
                    " The [" + settingPrefix + "supported_protocols] setting can be used to control this.");
            } else {
                assertThat(threadContext.getResponseHeaders(), not(hasKey("Warning")));
            }
        } finally {
            DeprecationLogger.removeThreadContext(threadContext);
        }

    }
}
