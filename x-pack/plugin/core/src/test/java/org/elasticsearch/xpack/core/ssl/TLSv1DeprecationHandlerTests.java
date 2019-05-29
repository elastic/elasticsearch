/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.SSLSession;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TLSv1DeprecationHandlerTests extends ESTestCase {

    public void testWithDefaultTlsProtocolsAndTLSv1() {
        final String prefix = randomAlphaOfLengthBetween(4, 8) + ".ssl.";
        final Settings.Builder builder = Settings.builder();
        builder.put(prefix + "enabled", true);
        final TLSv1DeprecationHandler handler = new TLSv1DeprecationHandler(prefix, builder.build(), logger);
        assertThat(handler.shouldLogWarnings(), is(true));

        final SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getProtocol()).thenReturn("TLSv1");
        handler.checkAndLog(sslSession, () -> "TLS deprecation test");

        assertWarnings("a TLS v1.0 session was used for [TLS deprecation test]," +
            " this protocol will be disabled by default in a future version." +
            " The [" + prefix + "supported_protocols] setting can be used to control this.");
    }

    public void testWithCustomisedTlsProtocolsAndTLSv1() {
        final String prefix = randomAlphaOfLengthBetween(4, 8) + ".ssl.";
        final Settings.Builder builder = Settings.builder();
        builder.put(prefix + "enabled", true);
        builder.putList(prefix + "supported_protocols", randomSubsetOf(randomIntBetween(1, 3), "TLSv1", "TLSv1.1", "TLSv1.2"));
        final TLSv1DeprecationHandler handler = new TLSv1DeprecationHandler(prefix, builder.build(), logger);
        assertThat(handler.shouldLogWarnings(), is(false));

        final SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getProtocol()).thenReturn("TLSv1");
        handler.checkAndLog(sslSession, () -> "TLS deprecation test");

        // The test base class will enforce that there are no deprecation warnings
    }

    public void testWithDefaultTlsProtocolsAndRecentTLS() {
        final String prefix = randomAlphaOfLengthBetween(4, 8) + ".ssl.";
        final Settings.Builder builder = Settings.builder();
        builder.put(prefix + "enabled", true);
        final TLSv1DeprecationHandler handler = new TLSv1DeprecationHandler(prefix, builder.build(), logger);
        assertThat(handler.shouldLogWarnings(), is(true));

        final SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getProtocol()).thenReturn(randomFrom("TLSv1.1", "TLSv1.2"));
        handler.checkAndLog(sslSession, () -> "TLS deprecation test");

        // The test base class will enforce that there are no deprecation warnings
    }

    public void testWithCustomisedTlsProtocolsAndRecentTLS() {
        final String prefix = randomAlphaOfLengthBetween(4, 8) + ".ssl.";
        final Settings.Builder builder = Settings.builder();
        builder.put(prefix + "enabled", true);
        builder.putList(prefix + "supported_protocols", randomSubsetOf(randomIntBetween(1, 3), "TLSv1", "TLSv1.1", "TLSv1.2"));
        final TLSv1DeprecationHandler handler = new TLSv1DeprecationHandler(prefix, builder.build(), logger);
        assertThat(handler.shouldLogWarnings(), is(false));

        final SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getProtocol()).thenReturn(randomFrom("TLSv1.1", "TLSv1.2"));
        handler.checkAndLog(sslSession, () -> "TLS deprecation test");

        // The test base class will enforce that there are no deprecation warnings
    }
}
