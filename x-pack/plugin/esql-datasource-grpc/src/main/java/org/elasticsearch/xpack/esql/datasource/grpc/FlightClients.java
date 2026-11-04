/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The one place that builds a Flight client, so that transport security is decided in one place and cannot be forgotten by a call
 * site. Whether a connection uses TLS is fixed by the scheme of the endpoint ({@code flight+tls} or {@code grpc+tls}) and never by
 * anything a satellite answers: a location in a flight info can change the host, not downgrade the connection. The certificate
 * authority is a data source setting given as PEM text, so no file is read; without it the JVM's trust store applies.
 */
final class FlightClients {

    static final String CA_CERTIFICATE = "ca_certificate";
    static final String SERVER_NAME = "server_name";
    static final String TOKEN = "token";
    static final String CREDENTIALS = "credentials";

    /** The call header that carries the dataset's settings (protocol section 11): base64url, no padding, of a JSON object. */
    static final String DATASET_SETTINGS_HEADER = "x-esql-dataset-settings";
    /** The call header that carries the data source's {@code credentials} setting, in the same encoding; only over TLS. */
    static final String CREDENTIALS_HEADER = "x-esql-credentials";

    /** The most each of the two headers may hold: gRPC allows about 8 KiB of metadata in all, and a bearer token goes with them. */
    static final int MAX_HEADER_VALUE_CHARS = 2048;

    /** The longest bearer token: with the two headers above it keeps the metadata of a call under the 8 KiB gRPC allows. */
    static final int MAX_TOKEN_CHARS = 1024;

    /** The data source settings that only mean something on a TLS connection: a credential is never sent in clear. */
    static final Set<String> TLS_SETTINGS = Set.of(CA_CERTIFICATE, SERVER_NAME, TOKEN, CREDENTIALS);

    /** The settings that are credentials: stored encrypted, never shown, never part of a fragment, an option or a log line. */
    static final Set<String> SECRET_SETTINGS = Set.of(TOKEN, CREDENTIALS);

    private static final String TLS_SUFFIX = "+tls";

    /**
     * How to reach an endpoint: plain, or TLS with the given trust; with a bearer token and the {@code credentials} of the data
     * source when it has them (TLS only); and with the dataset's settings, which go in a header on every call. Everything but
     * {@code tls} may be null. The secrets are left out of {@code toString}.
     */
    record Transport(boolean tls, String caCertificate, String serverName, String token, String credentials, String datasetSettings) {

        static final Transport PLAIN = new Transport(false, null, null, null, null, null);

        Transport(boolean tls, String caCertificate, String serverName, String token) {
            this(tls, caCertificate, serverName, token, null, null);
        }

        Transport(boolean tls, String caCertificate, String serverName) {
            this(tls, caCertificate, serverName, null, null, null);
        }

        /**
         * What tells one reader of a satellite from another in a cache keyed by endpoint and table: a hash of the secrets and of
         * the dataset settings, so that none of them is kept there. Empty without any.
         */
        String cacheScope() {
            if (token == null && credentials == null && datasetSettings == null) {
                return "";
            }
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                for (String part : new String[] { token, credentials, datasetSettings }) {
                    // an absent part and a part that is the text "-" must not look alike: a marker and the length come first
                    digest.update((part == null ? "N" : "S" + part.length() + ":" + part).getBytes(StandardCharsets.UTF_8));
                }
                return "#" + HexFormat.of().formatHex(digest.digest(), 0, 8);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public String toString() {
            return "Transport[tls="
                + tls
                + ", serverName="
                + serverName
                + ", token="
                + (token == null ? "none" : "<redacted>")
                + ", credentials="
                + (credentials == null ? "none" : "<redacted>")
                + "]";
        }
    }

    private FlightClients() {}

    /** Plain only for exactly {@code flight} and {@code grpc}: any other scheme is refused rather than read in clear. */
    static boolean isTls(String scheme) {
        if (scheme != null && scheme.endsWith(TLS_SUFFIX)) {
            return true;
        }
        if ("flight".equals(scheme) || "grpc".equals(scheme)) {
            return false;
        }
        throw new IllegalArgumentException("unsupported Flight scheme [" + scheme + "]");
    }

    /**
     * The transport for {@code endpoint}, with the TLS settings of the data source in {@code config}. A dataset's config holds
     * them under {@code _datasource}; a flat config is read too, for the callers that build one by hand.
     */
    static Transport transport(String endpoint, Map<String, Object> config) {
        String settings = encodeDatasetSettings(FlightConnectorFactory.optionsOf(config == null ? Map.of() : config));
        if (isTls(URI.create(endpoint).getScheme()) == false) {
            return settings == null ? Transport.PLAIN : new Transport(false, null, null, null, null, settings);
        }
        return new Transport(
            true,
            setting(config, CA_CERTIFICATE),
            setting(config, SERVER_NAME),
            setting(config, TOKEN),
            encodeCredentials(setting(config, CREDENTIALS)),
            settings
        );
    }

    /**
     * The dataset's settings as the value of {@link #DATASET_SETTINGS_HEADER}: a JSON object, base64url without padding; null when
     * there are none. They are scalars (the validator checks), and all of them are sent: which are table parameters and which are
     * source options is declared by the satellite, and ES cannot know before it has read the declaration.
     */
    static String encodeDatasetSettings(Map<String, Object> settings) {
        if (settings.isEmpty()) {
            return null;
        }
        return encodeHeaderValue(settings);
    }

    /** The {@code credentials} setting (a JSON object) as the value of {@link #CREDENTIALS_HEADER}; null when there are none. */
    private static String encodeCredentials(String credentials) {
        return credentials == null
            ? null
            : Base64.getUrlEncoder().withoutPadding().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    static String encodeHeaderValue(Map<String, Object> value) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            byte[] json = BytesReference.toBytes(BytesReference.bytes(builder.map(new TreeMap<>(value))));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String setting(Map<String, Object> config, String name) {
        if (config == null) {
            return null;
        }
        Object value = config.get(name);
        if (value == null && config.get(ExternalSourceResolver.DATASOURCE_CONFIG_KEY) instanceof Map<?, ?> datasource) {
            value = datasource.get(name);
        }
        if (value == null) {
            return null;
        }
        // a SecureString from the secret machinery is fine; a value still encrypted is not a setting, and must never be sent
        if (value instanceof String == false && value instanceof CharSequence == false) {
            throw new IllegalStateException("the data source setting [" + name + "] has not been decrypted");
        }
        return value.toString();
    }

    /**
     * The client for {@code host:port}, on {@code allocator}, which the client keeps and the caller closes. The host is checked
     * against {@code hosts} here, the one place every connection is made, as well as when a dataset is registered.
     */
    static FlightClient build(BufferAllocator allocator, String host, int port, Transport transport, FlightGuard hosts) {
        hosts.check(host);
        int maxMessage = hosts.limits().maxMessageBytesInt();
        FlightClient.Builder builder = FlightClient.builder(
            allocator,
            transport.tls() ? Location.forGrpcTls(host, port) : Location.forGrpcInsecure(host, port)
        ).maxInboundMessageSize(maxMessage);
        // a middleware, not a call option: it covers every call of the client, the result stream included
        builder.intercept(headers(transport));
        if (transport.tls() == false) {
            return builder.build();
        }
        if (transport.serverName() != null) {
            builder.overrideHostname(transport.serverName());
        }
        if (transport.caCertificate() == null) {
            return builder.build();
        }
        try (InputStream ca = new ByteArrayInputStream(transport.caCertificate().getBytes(StandardCharsets.UTF_8))) {
            return builder.trustedCertificates(ca).build();
        } catch (IOException e) {
            throw new IllegalStateException("cannot use the certificate authority of the data source", e);
        }
    }

    private static FlightClientMiddleware.Factory headers(Transport transport) {
        String authorization = transport.token() == null ? null : "Bearer " + transport.token();
        return info -> new FlightClientMiddleware() {
            @Override
            public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                if (authorization != null) {
                    outgoingHeaders.insert("authorization", authorization);
                }
                if (transport.credentials() != null) {
                    outgoingHeaders.insert(CREDENTIALS_HEADER, transport.credentials());
                }
                if (transport.datasetSettings() != null) {
                    outgoingHeaders.insert(DATASET_SETTINGS_HEADER, transport.datasetSettings());
                }
            }

            @Override
            public void onHeadersReceived(CallHeaders incomingHeaders) {}

            @Override
            public void onCallCompleted(CallStatus status) {}
        };
    }

    /**
     * The allocator for the calls that plan a read (schema, declaration, flight info). It has a limit on the Arrow buffers a call
     * holds; most of what these calls decode lives on the heap, where the message size limit is what bounds it. It is not accounted
     * by a circuit breaker, which a plan step has no handle on.
     */
    static BufferAllocator planningAllocator(FlightGuard guard) {
        return new RootAllocator(2 * guard.limits().maxMessageBytes());
    }

    /**
     * Text a satellite sent, made fit for an error message: control characters removed, at most 256 characters, and marked as the
     * satellite's, because ES does not vouch for it.
     */
    static String satelliteText(String text) {
        if (text == null || text.isBlank()) {
            return "(no message from the satellite)";
        }
        StringBuilder clean = new StringBuilder();
        text.codePoints().limit(256).forEach(c -> {
            int type = Character.getType(c);
            // control, format (bidi marks), line and paragraph separators and unassigned or surrogate code points become spaces
            boolean unsafe = type == Character.CONTROL
                || type == Character.FORMAT
                || type == Character.LINE_SEPARATOR
                || type == Character.PARAGRAPH_SEPARATOR
                || type == Character.SURROGATE
                || type == Character.UNASSIGNED;
            clean.appendCodePoint(unsafe ? ' ' : c);
        });
        return "(the satellite says: " + clean.toString().strip() + (text.codePointCount(0, text.length()) > 256 ? "...)" : ")");
    }

    /** The error for a satellite that did not accept the credentials of the data source, or null for any other failure. */
    static IllegalArgumentException authenticationFailure(Throwable t) {
        if (t instanceof FlightRuntimeException e) {
            FlightStatusCode code = e.status().code();
            if (code == FlightStatusCode.UNAUTHENTICATED || code == FlightStatusCode.UNAUTHORIZED) {
                // the satellite's own text is not repeated: it is not ours to vouch for, and a credential must not be echoed
                return new IllegalArgumentException(
                    "the satellite did not accept the credentials of the data source ["
                        + (code == FlightStatusCode.UNAUTHENTICATED ? "UNAUTHENTICATED" : "PERMISSION_DENIED")
                        + "]"
                );
            }
        }
        return null;
    }

    /** Whether {@code pem} holds at least one X.509 certificate; says why not otherwise. */
    static void checkCertificates(String pem) {
        try (InputStream in = new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8))) {
            if (CertificateFactory.getInstance("X.509").generateCertificates(in).isEmpty()) {
                throw new IllegalArgumentException("[" + CA_CERTIFICATE + "] holds no certificate");
            }
        } catch (CertificateException | IOException e) {
            throw new IllegalArgumentException("[" + CA_CERTIFICATE + "] is not a PEM encoded X.509 certificate: " + e.getMessage(), e);
        }
    }
}
