/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.common.ssl.PemTrustConfig;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Standalone HTTPS server that mimics the workload-identity-issuer's {@code POST /token} endpoint
 * for local development and manual end-to-end testing of the workload-identity client. Reuses the
 * checked-in mTLS material under {@code src/test/resources/org/elasticsearch/workloadidentity}
 * (CA, issuer cert/key, node client cert/key) so that an Elasticsearch dev cluster pointed at
 * this fixture can complete the same mTLS handshake the production transport relies on. Client auth
 * (mTLS) is always required, matching the production transport.
 *
 * <h2>Behavior</h2>
 * Every {@code POST /token} parses the {@code aud} value from the request body and returns the
 * JWT registered for that audience:
 * <pre>{@code
 * { "token": "<jwt-for-aud>", "expires_at": <jwt exp claim> }
 * }</pre>
 * Audience-to-JWT mappings are supplied with one or more repeatable {@code --token-for <aud>=<file>}
 * flags (at least one is required), where {@code <file>} holds a single signed JWT. This lets the
 * fixture serve the real, per-cloud-provider JWTs (e.g. the AWS / GCP / Azure tokens minted by an
 * external IdP) keyed by the audience each datasource plugin requests. Each JWT must carry a
 * decodable {@code exp} claim (every real IdP mints one); the fixture rejects tokens without one at
 * startup rather than inventing an expiry. A request for an unmapped audience returns {@code 404}, as
 * does anything other than {@code POST /token}.
 *
 * <h2>Running</h2>
 * From the {@code modules/workload-identity} project:
 * <pre>{@code
 * ./gradlew :modules:workload-identity:runTestIssuer --args='--port 8443 \
 *   --token-for "sts.amazonaws.com=/tmp/aws-jwt.txt" \
 *   --token-for "api://AzureADTokenExchange=/tmp/azure-jwt.txt"'
 * }</pre>
 * On startup the server prints its address, the registered audiences, and the
 * {@code workload_identity.*} settings a node should set in {@code elasticsearch.yml} to
 * authenticate against it.
 *
 * <h2>Caveats</h2>
 * Strictly a local dev / manual-test fixture: the keystore password is a literal in the source and
 * the bind host is loopback only. Not appropriate for any environment a real customer can reach.
 * The JWTs it serves are whatever the operator points it at; those files are live bearer
 * credentials and must never be committed to source control.
 */
@SuppressForbidden(reason = "use the JDK https server as a controllable mTLS workload-identity-issuer fixture")
public final class LocalTestWorkloadIdentityIssuer {

    /**
     * Resource directory holding the test PEM material. Mirrors the layout used by
     * {@code HttpsWorkloadIdentityIssuerClientTests}; see the README in that directory for
     * regeneration instructions.
     */
    private static final String RESOURCE_BASE = "/org/elasticsearch/workloadidentity/";

    /**
     * Passphrase for the issuer key in {@code issuer/issuer.key}. Pinned literal so the fixture is
     * self-contained: the key material is committed to the repo for tests, so the password being
     * a literal here adds no real secrecy to a private resource.
     */
    private static final char[] ISSUER_KEY_PASSWORD = "http-password".toCharArray();

    public static void main(String[] args) throws Exception {
        final Args parsed = Args.parse(args);
        if (parsed.help) {
            System.out.println(Args.helpText());
            return;
        }

        final Map<String, RegisteredToken> tokensByAudience = parsed.loadTokens();
        if (tokensByAudience.isEmpty()) {
            System.err.println("at least one --token-for <aud>=<file> mapping is required\n\n" + Args.helpText());
            return;
        }

        final Path issuerCert = resourcePath("issuer/issuer.crt");
        final Path issuerKey = resourcePath("issuer/issuer.key");
        final Path caCert = resourcePath("ca.crt");

        final SSLContext sslContext = buildSslContext(issuerCert, issuerKey, caCert);

        final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), parsed.port);
        final HttpsServer server = MockHttpServer.createHttps(address, 0);
        server.setHttpsConfigurator(new ClientAuthHttpsConfigurator(sslContext));
        server.createContext("/token", new TokenHandler(tokensByAudience));
        server.start();

        printStartupBanner(server, tokensByAudience, caCert);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[workload-identity-issuer] shutting down...");
            server.stop(0);
        }, "wi-issuer-shutdown"));

        // Block forever; let the shutdown hook handle Ctrl-C.
        Thread.currentThread().join();
    }

    private static void printStartupBanner(HttpsServer server, Map<String, RegisteredToken> tokensByAudience, Path caCert) {
        final InetSocketAddress bound = server.getAddress();
        final String url = "https://localhost:" + bound.getPort();
        final Path testResources = caCert.getParent();
        final Path nodeCert = testResources.resolve("node").resolve("node.crt");
        final Path nodeKey = testResources.resolve("node").resolve("node.key");

        final StringBuilder audiences = new StringBuilder();
        for (Map.Entry<String, RegisteredToken> entry : tokensByAudience.entrySet()) {
            audiences.append("\n      - ").append(entry.getKey()).append(" -> exp=").append(entry.getValue().expiresAt());
        }

        final String banner = """

            =====================================================================
              Local workload-identity-issuer test fixture
            =====================================================================
              Listening on: %s   (POST /token, mTLS required)
              Audiences:%s

              Point a node at this issuer (config/elasticsearch.yml):
                workload_identity.issuer.url: %s
                workload_identity.ssl.certificate_authorities: [ %s ]
                workload_identity.ssl.certificate: %s
                workload_identity.ssl.key:         %s
              Keystore: workload_identity.ssl.secure_key_passphrase = client-password

              Press Ctrl-C to stop.
            =====================================================================
            """.formatted(url, audiences.toString(), url, caCert, nodeCert, nodeKey);
        System.out.println(banner);
    }

    private static SSLContext buildSslContext(Path cert, Path key, Path ca) throws Exception {
        // PemKeyConfig / PemTrustConfig accept absolute paths and use the configBase only to
        // resolve relative ones; we already pass absolute Paths derived from the classpath URL.
        final Path configBase = cert.getParent().getParent();
        final PemKeyConfig keyConfig = new PemKeyConfig(cert.toString(), key.toString(), ISSUER_KEY_PASSWORD, configBase);
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();

        final X509ExtendedTrustManager trustManager = new PemTrustConfig(Collections.singletonList(ca.toString()), configBase)
            .createTrustManager();

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(new KeyManager[] { keyManager }, new TrustManager[] { trustManager }, null);
        return sslContext;
    }

    private static Path resourcePath(String name) {
        final URL url = LocalTestWorkloadIdentityIssuer.class.getResource(RESOURCE_BASE + name);
        if (url == null) {
            throw new IllegalStateException(
                "cannot find test PEM resource on the classpath: "
                    + RESOURCE_BASE
                    + name
                    + " (run via './gradlew :modules:workload-identity:runTestIssuer' so test resources are on the classpath)"
            );
        }
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("cannot resolve PEM resource path from URL: " + url, e);
        }
    }

    record RegisteredToken(String jwt, long expiresAt) {}

    /**
     * Decodes the {@code exp} (expiry, epoch seconds) claim from a JWT's payload segment.
     * Dependency-free: the payload is the middle base64url segment, decoded and scanned for the
     * {@code exp} field.
     */
    static long decodeExp(String jwt) throws IOException {
        final String[] segments = jwt.split("\\.");
        if (segments.length != 3) {
            throw new IllegalArgumentException("not a three-segment JWT");
        }
        final byte[] payload = Base64.getUrlDecoder().decode(segments[1]);
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, payload)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("JWT payload is not a JSON object");
            }
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                    continue;
                }
                final String field = parser.currentName();
                parser.nextToken();
                if ("exp".equals(field)) {
                    return parser.longValue();
                } else {
                    parser.skipChildren();
                }
            }
            throw new IllegalArgumentException("JWT payload has no exp claim");
        }
    }

    /**
     * Handler for {@code POST /token}. Logs the requested {@code aud} value, then writes the JWT
     * registered for that audience. The {@code expires_at} field is the served JWT's {@code exp}
     * claim (validated and decoded at startup) so the client's token cache treats the token as fresh
     * for its true lifetime. An unmapped audience returns {@code 404}.
     */
    @SuppressForbidden(
        reason = "uses the JDK com.sun.net.httpserver request/response types and prints request diagnostics to stdout for this fixture"
    )
    private static final class TokenHandler implements HttpHandler {
        private final Map<String, RegisteredToken> tokensByAudience;

        TokenHandler(Map<String, RegisteredToken> tokensByAudience) {
            this.tokensByAudience = tokensByAudience;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) == false) {
                    writePlain(exchange, 405, "method not allowed");
                    return;
                }

                final byte[] requestBody = exchange.getRequestBody().readAllBytes();
                final String audience = parseAudience(requestBody);

                final RegisteredToken registered = tokensByAudience.get(audience);
                if (registered == null) {
                    System.out.printf("[workload-identity-issuer] POST /token  aud=%s  -> 404 (no JWT registered)%n", audience);
                    writePlain(exchange, 404, "no JWT registered for audience: " + audience);
                    return;
                }

                final long expiresAtSeconds = registered.expiresAt();
                System.out.printf("[workload-identity-issuer] POST /token  aud=%s  -> 200 (expires_at=%d)%n", audience, expiresAtSeconds);

                final String response = "{\"token\":\"" + registered.jwt() + "\",\"expires_at\":" + expiresAtSeconds + "}";
                final byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } finally {
                exchange.close();
            }
        }

        private static void writePlain(HttpExchange exchange, int status, String message) throws IOException {
            final byte[] body = message.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }

        private static String parseAudience(byte[] body) {
            if (body.length == 0) {
                return "<empty body>";
            }
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, body)) {
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    return "<not a JSON object>";
                }
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                        continue;
                    }
                    final String field = parser.currentName();
                    parser.nextToken();
                    if ("aud".equals(field)) {
                        return parser.text();
                    } else {
                        parser.skipChildren();
                    }
                }
                return "<missing>";
            } catch (Exception e) {
                return "<parse error: " + e.getMessage() + ">";
            }
        }
    }

    @SuppressForbidden(reason = "JDK https server")
    private static final class ClientAuthHttpsConfigurator extends HttpsConfigurator {
        ClientAuthHttpsConfigurator(SSLContext sslContext) {
            super(sslContext);
        }

        @Override
        public void configure(HttpsParameters params) {
            // mTLS: the node must present the client cert, mirroring the production transport.
            params.setNeedClientAuth(true);
        }
    }

    /**
     * Parsed CLI arguments. Kept as a record-like data carrier with simple, hand-rolled parsing so
     * the fixture has zero command-line-library dependencies. All flags are optional except that at
     * least one {@code --token-for} mapping must be supplied.
     */
    private static final class Args {
        int port = 8443;
        // Insertion-ordered so the startup banner lists audiences in the order they were supplied.
        final Map<String, Path> tokenFiles = new LinkedHashMap<>();
        boolean help = false;

        /**
         * Reads each registered JWT file into memory once at startup, trimming surrounding
         * whitespace (so a trailing newline in the file does not corrupt the token).
         */
        Map<String, RegisteredToken> loadTokens() throws IOException {
            final Map<String, RegisteredToken> tokens = new LinkedHashMap<>();
            for (Map.Entry<String, Path> entry : tokenFiles.entrySet()) {
                final String token = Files.readString(entry.getValue(), StandardCharsets.UTF_8).trim();
                if (token.isEmpty()) {
                    throw new IllegalArgumentException("JWT file is empty: " + entry.getValue());
                }
                tokens.put(entry.getKey(), new RegisteredToken(token, decodeExp(token)));
            }
            return tokens;
        }

        static Args parse(String[] argv) {
            final Args args = new Args();
            for (int i = 0; i < argv.length; i++) {
                final String arg = argv[i];
                switch (arg) {
                    case "--help", "-h" -> args.help = true;
                    case "--port" -> args.port = Integer.parseInt(requireValue(argv, ++i, arg));
                    case "--token-for" -> addTokenFor(args, requireValue(argv, ++i, arg));
                    default -> throw new IllegalArgumentException("unknown argument: " + arg + "\n\n" + helpText());
                }
            }
            return args;
        }

        /**
         * Parses a {@code <aud>=<file>} mapping. The audience may itself contain {@code =}
         * characters (GCP workload-identity-pool audiences do not, but be lenient), so we split on
         * the first {@code =} only.
         */
        private static void addTokenFor(Args args, String spec) {
            final int eq = spec.indexOf('=');
            if (eq <= 0 || eq == spec.length() - 1) {
                throw new IllegalArgumentException("--token-for expects <aud>=<file>, got: " + spec + "\n\n" + helpText());
            }
            final String audience = spec.substring(0, eq);
            final Path file = PathUtils.get(spec.substring(eq + 1));
            args.tokenFiles.put(audience, file);
        }

        private static String requireValue(String[] argv, int i, String flag) {
            if (i >= argv.length) {
                throw new IllegalArgumentException("flag " + flag + " requires a value\n\n" + helpText());
            }
            return argv[i];
        }

        static String helpText() {
            return """
                LocalTestWorkloadIdentityIssuer - mTLS workload-identity-issuer fixture for local dev

                Flags:
                  --port <int>                listen port (default: 8443; 0 picks an ephemeral port)
                  --token-for <aud>=<file>    serve the JWT in <file> for requests with this aud (repeatable, at least one required)
                  -h, --help                  show this message
                """;
        }
    }

    private LocalTestWorkloadIdentityIssuer() {}
}
