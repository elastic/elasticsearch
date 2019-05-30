/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.network.NetworkAddress;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocketFactory;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * This class exists to help explain SSL errors in human readable terms.
 * The JDK's default SSL exception messages tend to reflect their low level implementation details, and are difficult to understand
 * if you are not familiar with the specifics of JSSE and TLS.
 * This class translates these exceptions into messages that intend to explain the high level cause, and the steps that can be taken to
 * resolve the problem.
 */
public class SslExceptionExplainer {

    private final String settingContext;
    private final ConnectionMode mode;
    private final InetSocketAddress remoteAddress;
    private final List<String> jvmCipherSuites;
    private final List<String> configuredCipherSuites;
    private final List<String> remoteCipherSuites;

    public enum ConnectionMode {
        CLIENT, SERVER
    }

    public static SslExceptionExplainer clientExplainer(String settingContext, InetSocketAddress remoteAddress,
                                                        SSLSocketFactory socketFactory) {
        return new SslExceptionExplainer(
            settingContext,
            ConnectionMode.CLIENT,
            remoteAddress,
            Arrays.asList(socketFactory.getDefaultCipherSuites()),
            Arrays.asList(socketFactory.getSupportedCipherSuites()),
            null
        );
    }

    public static SslExceptionExplainer serverExplainer(String settingContext, InetSocketAddress remoteAddress, SSLEngine engine) {
        final List<String> clientCipherSuites;
        if (engine instanceof SniffingSSLEngine) {
            clientCipherSuites = ((SniffingSSLEngine) engine).getRemoteCipherSuites();
        } else {
            clientCipherSuites = null;
        }
        return new SslExceptionExplainer(
            settingContext,
            ConnectionMode.SERVER,
            remoteAddress,
            TlsCipherSuites.JVM_SUPPORTED_CIPHER_SUITES,
            Arrays.asList(engine.getEnabledCipherSuites()),
            clientCipherSuites
        );
    }

    public SslExceptionExplainer(String settingContext, ConnectionMode mode, InetSocketAddress remoteAddress,
                                 List<String> jvmCipherSuites, List<String> configuredCipherSuites,
                                 @Nullable List<String> remoteCipherSuites) {
        if (settingContext.endsWith(".ssl") == false) {
            throw new IllegalArgumentException("The setting context [" + settingContext + "] must end in '.ssl'");
        }
        this.settingContext = settingContext;
        this.mode = mode;
        this.remoteAddress = remoteAddress;
        this.jvmCipherSuites = List.copyOf(jvmCipherSuites);
        this.configuredCipherSuites = List.copyOf(configuredCipherSuites);
        this.remoteCipherSuites = remoteCipherSuites;
    }

    /**
     * Provide a human readable explanation for the given exception, or {@code null} if it cannot be translated.
     */
    public String explain(SSLException sslException) {
        Throwable root = getRootCause(sslException);
        if (root.getMessage().contains("no cipher suites in common")) {
            return explainCipherSuites().toString();
        }
        if (sslException instanceof SSLHandshakeException) {
            if (root instanceof EOFException) {
                return "The remote " + (mode == ConnectionMode.CLIENT ? "server" : "client") + " (" + addressString() +
                    ") closed the connection unexpectedly during SSL handshake";
            } else if (root == sslException) {
                return "The remote " + (mode == ConnectionMode.CLIENT ? "server" : "client") + " (" + addressString() +
                    ") rejected our SSL handshake";
            }
        }
        return null;
    }

    public String addressString() {
        return remoteAddress == null ? "{unknown address}" : NetworkAddress.format(remoteAddress);
    }

    private CharSequence explainCipherSuites() {
        final StringBuilder message = new StringBuilder()
            .append("The SSL connection ")
            .append(mode == ConnectionMode.CLIENT ? "to" : "from")
            .append(" [")
            .append(addressString())
            .append("] failed because ");
        if (mode == ConnectionMode.CLIENT) {
            message.append("the server did not accept any of our configured ciphers.");
        } else {
            message.append("we do not accept any of the client's supported ciphers.");
        }
        message.append('\n');
        if (remoteCipherSuites != null) {
            message.append("The remote ")
                .append(mode == ConnectionMode.CLIENT ? "server" : "client")
                .append(" supports:\n");
            for (String suite : this.remoteCipherSuites) {
                message.append(" * ").append(suite);
                if (this.jvmCipherSuites.contains(suite)) {
                    message.append(" (can be supported on this JVM)");
                }
                message.append("\n");
            }
        }

        message.append("We are configured to support:\n");
        for (String suite : this.configuredCipherSuites) {
            message.append(" * ").append(suite).append("\n");
        }
        if (remoteCipherSuites == null) {
            message.append("This JVM can also support:\n");
            for (String suite : this.jvmCipherSuites) {
                if (this.configuredCipherSuites.contains(suite) == false) {
                    message.append(" * ").append(suite).append("\n");
                }
            }
        }
        message.append("The setting [")
            .append(settingContext)
            .append(".cipher_suites] can be used to configure which ciphers will be used for this type of connection");
        return message;
    }

    private static Throwable getRootCause(SSLException e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
