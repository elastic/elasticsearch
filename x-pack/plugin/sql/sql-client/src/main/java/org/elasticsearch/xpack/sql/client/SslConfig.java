/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class SslConfig {

    public static final String SSL = "ssl";
    private static final String SSL_DEFAULT = "false";

    public static final String SSL_PROTOCOL = "ssl.protocol";
    private static final String SSL_PROTOCOL_DEFAULT = "TLS"; // SSL alternative

    public static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
    private static final String SSL_KEYSTORE_LOCATION_DEFAULT = "";

    public static final String SSL_KEYSTORE_PASS = "ssl.keystore.pass";
    private static final String SSL_KEYSTORE_PASS_DEFAULT = "";

    public static final String SSL_KEYSTORE_TYPE = "ssl.keystore.type";
    private static final String SSL_KEYSTORE_TYPE_DEFAULT = "JKS"; // PCKS12

    public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
    private static final String SSL_TRUSTSTORE_LOCATION_DEFAULT = "";

    public static final String SSL_TRUSTSTORE_PASS = "ssl.truststore.pass";
    private static final String SSL_TRUSTSTORE_PASS_DEFAULT = "";

    public static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";
    private static final String SSL_TRUSTSTORE_TYPE_DEFAULT = "JKS";

    static final Set<String> OPTION_NAMES = new LinkedHashSet<>(
        Arrays.asList(
            SSL,
            SSL_PROTOCOL,
            SSL_KEYSTORE_LOCATION,
            SSL_KEYSTORE_PASS,
            SSL_KEYSTORE_TYPE,
            SSL_TRUSTSTORE_LOCATION,
            SSL_TRUSTSTORE_PASS,
            SSL_TRUSTSTORE_TYPE
        )
    );

    private final boolean enabled;
    private final String protocol, keystoreLocation, keystorePass, keystoreType;
    private final String truststoreLocation, truststorePass, truststoreType;

    private final SSLContext sslContext;

    public SslConfig(Properties settings, URI baseURI) {
        boolean isSchemaPresent = baseURI.getScheme() != null;
        boolean isSSLPropertyPresent = settings.getProperty(SSL) != null;
        boolean isHttpsScheme = "https".equals(baseURI.getScheme());

        if (isSSLPropertyPresent == false && isSchemaPresent == false) {
            enabled = StringUtils.parseBoolean(SSL_DEFAULT);
        } else {
            if (isSSLPropertyPresent && isHttpsScheme && StringUtils.parseBoolean(settings.getProperty(SSL)) == false) {
                throw new ClientException("Cannot enable SSL: HTTPS protocol being used in the URL and SSL disabled in properties");
            }
            enabled = isHttpsScheme || StringUtils.parseBoolean(settings.getProperty(SSL, SSL_DEFAULT));
        }
        protocol = settings.getProperty(SSL_PROTOCOL, SSL_PROTOCOL_DEFAULT);
        keystoreLocation = settings.getProperty(SSL_KEYSTORE_LOCATION, SSL_KEYSTORE_LOCATION_DEFAULT);
        keystorePass = settings.getProperty(SSL_KEYSTORE_PASS, SSL_KEYSTORE_PASS_DEFAULT);
        keystoreType = settings.getProperty(SSL_KEYSTORE_TYPE, SSL_KEYSTORE_TYPE_DEFAULT);
        truststoreLocation = settings.getProperty(SSL_TRUSTSTORE_LOCATION, SSL_TRUSTSTORE_LOCATION_DEFAULT);
        truststorePass = settings.getProperty(SSL_TRUSTSTORE_PASS, SSL_TRUSTSTORE_PASS_DEFAULT);
        truststoreType = settings.getProperty(SSL_TRUSTSTORE_TYPE, SSL_TRUSTSTORE_TYPE_DEFAULT);

        sslContext = enabled ? createSSLContext() : null;
    }

    // ssl
    boolean isEnabled() {
        return enabled;
    }

    SSLSocketFactory sslSocketFactory() {
        return sslContext.getSocketFactory();
    }

    private SSLContext createSSLContext() {
        SSLContext ctx;
        try {
            ctx = SSLContext.getInstance(protocol);
            ctx.init(loadKeyManagers(), loadTrustManagers(), null);
        } catch (Exception ex) {
            throw new ClientException("Failed to initialize SSL - " + ex.getMessage(), ex);
        }

        return ctx;
    }

    private KeyManager[] loadKeyManagers() throws GeneralSecurityException, IOException {
        if (StringUtils.hasText(keystoreLocation) == false) {
            return null;
        }

        char[] pass = (StringUtils.hasText(keystorePass) ? keystorePass.trim().toCharArray() : null);
        KeyStore keyStore = loadKeyStore(keystoreLocation, pass, keystoreType);
        KeyManagerFactory kmFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmFactory.init(keyStore, pass);
        return kmFactory.getKeyManagers();
    }

    private KeyStore loadKeyStore(String source, char[] pass, String keyStoreType) throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        Path path = Paths.get(source);

        if (Files.exists(path) == false) {
            throw new ClientException(
                "Expected to find keystore file at [" + source + "] but was unable to. Make sure you have specified a valid URI."
            );
        }

        try (InputStream in = Files.newInputStream(Paths.get(source), StandardOpenOption.READ)) {
            keyStore.load(in, pass);
        } catch (Exception ex) {
            throw new ClientException("Cannot open keystore [" + source + "] - " + ex.getMessage(), ex);
        } finally {

        }
        return keyStore;
    }

    private TrustManager[] loadTrustManagers() throws GeneralSecurityException, IOException {
        KeyStore keyStore = null;

        if (StringUtils.hasText(truststoreLocation)) {
            char[] pass = (StringUtils.hasText(truststorePass) ? truststorePass.trim().toCharArray() : null);
            keyStore = loadKeyStore(truststoreLocation, pass, truststoreType);
        }

        TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmFactory.init(keyStore);
        return tmFactory.getTrustManagers();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SslConfig other = (SslConfig) obj;
        return Objects.equals(enabled, other.enabled)
            && Objects.equals(protocol, other.protocol)
            && Objects.equals(keystoreLocation, other.keystoreLocation)
            && Objects.equals(keystorePass, other.keystorePass)
            && Objects.equals(keystoreType, other.keystoreType)
            && Objects.equals(truststoreLocation, other.truststoreLocation)
            && Objects.equals(truststorePass, other.truststorePass)
            && Objects.equals(truststoreType, other.truststoreType);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
