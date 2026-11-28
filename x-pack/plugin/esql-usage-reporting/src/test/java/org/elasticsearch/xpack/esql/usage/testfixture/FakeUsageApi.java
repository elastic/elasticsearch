/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage.testfixture;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.common.ssl.PemTrustConfig;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.usage.UsageReportingSettings;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.TrustManager;

/**
 * A stand-in for the Elastic Usage API as a hosted deployment sees it: {@code POST /api/v1/usage} with a JSON array of usage records,
 * over HTTPS with a client certificate that the server requires and checks against a test CA. It exists so that the publisher and
 * the whole delivery path can be tested without Cloud, real certificates or a route that tests cannot reach.
 * <p>
 * It records every request that completed the TLS handshake (a client without a valid certificate never reaches the handler, so it
 * leaves no request, as with the real API), and answers according to a script that a test sets: one {@link Behavior} for every
 * request, or one-shot behaviors queued ahead of it. The statuses are those the contract of the collector classifies: 201 is the
 * only success the OpenAPI specification lists; 400 invalid; 401 and 403 authentication; 408, 429, 5xx and every status that the
 * pipeline does not document, 422 included, retryable (see {@code HttpUsagePublisher#classify}).
 * <p>
 * It does not deduplicate: a record that is sent again is stored again here, where the real API discards it by id. A test can show
 * that the same id was sent again, not that it was billed once.
 * <p>
 * The TLS material is in {@code src/test/resources/org/elasticsearch/xpack/esql/usage/testfixture} (see its README). It is checked in,
 * as for the other mutual TLS tests of the repository, so that no test depends on a certificate generator.
 */
@SuppressForbidden(reason = "use the JDK https server as a controllable Usage API fixture")
public final class FakeUsageApi implements Closeable {

    /** The path of the only endpoint of the Usage API that the collector calls. */
    public static final String PATH = "/api/v1/usage";

    /** Subject of the client certificate that {@link #clientCertificate()} points to; the identity the fixture sees. */
    public static final String CLIENT_SUBJECT = "CN=es-hosted-node";

    /** Subject of a second client certificate signed by the same CA, for tests of {@link Behavior#acceptOnlyClient}. */
    public static final String OTHER_CLIENT_SUBJECT = "CN=other-node";

    private static final String RESOURCE_BASE = "/org/elasticsearch/xpack/esql/usage/testfixture/";

    /**
     * What the fixture does with one request: the status to answer, whether to wait first, or whether to drop the connection
     * without answering. A reply is what the real API does under a given condition, never what the collector does about it.
     * {@code commit} says whether the API stored the records, which is independent of what the client saw: a stored batch whose
     * answer was lost is the case in which a retry with the same ids must not be billed twice.
     */
    public record Reply(int status, TimeValue delay, boolean dropConnection, boolean commit) {
        public Reply {
            if (dropConnection == false && (status < 200 || status > 599 || status == 204 || status == 304)) {
                throw new IllegalArgumentException("the fixture answers with a body, so it cannot answer with status [" + status + "]");
            }
        }

        /** Answers the status after storing the records if, and only if, the status is a 2xx. */
        public static Reply status(int status) {
            return new Reply(status, TimeValue.ZERO, false, status / 100 == 2);
        }
    }

    /** Decides the reply to a request; the request is already recorded when it is called. */
    @FunctionalInterface
    public interface Behavior {
        Reply reply(ReceivedRequest request);

        /** The only success of the API specification. */
        Behavior CREATED = request -> Reply.status(201);

        /** Answers every request with the given status, whatever it contains. */
        static Behavior always(int status) {
            return request -> Reply.status(status);
        }

        /**
         * Answers 400 to every batch that contains the record, and 201 to the others: what the real API does for a record that fails
         * validation, since the specification has no per-record answer.
         */
        static Behavior rejectRecord(String recordId) {
            return request -> request.recordIds().contains(recordId) ? Reply.status(400) : Reply.status(201);
        }

        /** Answers 201 after the given time, to exercise the request timeout of the client. */
        static Behavior slow(TimeValue delay) {
            return request -> new Reply(201, delay, false, true);
        }

        /** Closes the connection without any answer: a network failure as the client sees it. */
        static Behavior dropConnection() {
            return request -> new Reply(0, TimeValue.ZERO, true, false);
        }

        /**
         * Stores the records, then closes the connection without any answer: the client sees a network failure although the Usage API
         * has the batch, so the retry is a duplicate that the API must discard by id.
         */
        static Behavior acceptThenDropConnection() {
            return request -> new Reply(0, TimeValue.ZERO, true, true);
        }

        /** Answers 403 to every client whose certificate subject is not the given one, and 201 to that one. */
        static Behavior acceptOnlyClient(String subject) {
            return request -> subject.equals(request.clientSubject()) ? Reply.status(201) : Reply.status(403);
        }
    }

    /**
     * One request that the fixture received.
     *
     * @param clientSubject the subject of the client certificate, as the TLS session saw it
     * @param method        the HTTP method
     * @param path          the path of the request
     * @param body          the body as received
     * @param recordIds     the {@code id} of each record of the body, in order; empty if the body is not an array of objects
     * @param records       the records of the body as maps; empty if the body is not an array of objects
     */
    public record ReceivedRequest(
        String clientSubject,
        String method,
        String path,
        String body,
        List<String> recordIds,
        List<Map<String, Object>> records
    ) {}

    private final HttpsServer server;
    private final ExecutorService executor;
    private final Path directory;
    private final List<ReceivedRequest> requests = new CopyOnWriteArrayList<>();
    private final List<String> acceptedRecordIds = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<Behavior> oneShot = new ConcurrentLinkedQueue<>();
    private volatile Behavior behavior = Behavior.CREATED;

    private FakeUsageApi(HttpsServer server, ExecutorService executor, Path directory) {
        this.server = server;
        this.executor = executor;
        this.directory = directory;
    }

    /** Starts a server on a free loopback port that requires a client certificate signed by the test CA. */
    public static FakeUsageApi start() throws Exception {
        Path directory = resource("server.crt").getParent();
        SSLContext sslContext = serverSslContext(directory);
        HttpsServer server = MockHttpServer.createHttps(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setHttpsConfigurator(new RequireClientCertificate(sslContext));
        // the handler may sleep or drop the connection, so it must not run on the thread that accepts connections
        ExecutorService executor = Executors.newFixedThreadPool(4, EsExecutors.daemonThreadFactory("test", "fake-usage-api"));
        server.setExecutor(executor);
        FakeUsageApi api = new FakeUsageApi(server, executor, directory);
        server.createContext("/", api::handle);
        server.start();
        return api;
    }

    /**
     * The base URL of the server. The {@code xpack.esql.usage_reporting.url} setting is the whole endpoint, {@code url() + PATH}: see
     * {@link #pluginSettings()}.
     */
    public String url() {
        // the address the server is bound to, not the name "localhost", which may resolve to the other IP family; the server
        // certificate has the loopback addresses in its subject alternative names
        InetAddress address = server.getAddress().getAddress();
        String host = address instanceof Inet6Address ? "[" + address.getHostAddress() + "]" : address.getHostAddress();
        return "https://" + host + ":" + server.getAddress().getPort();
    }

    /** The PEM of the certificate whose subject is {@link #CLIENT_SUBJECT}, signed by the CA that this fixture trusts. */
    public Path clientCertificate() {
        return directory.resolve("client.crt");
    }

    /** The unencrypted PEM key of {@link #clientCertificate()}. */
    public Path clientKey() {
        return directory.resolve("client.key");
    }

    /** The PEM of the CA that signed the server certificate, for the client to trust. */
    public Path certificateAuthority() {
        return directory.resolve("ca.crt");
    }

    /** A second certificate of the same CA, with subject {@link #OTHER_CLIENT_SUBJECT}. */
    public Path otherClientCertificate() {
        return directory.resolve("other-client.crt");
    }

    public Path otherClientKey() {
        return directory.resolve("other-client.key");
    }

    /**
     * A certificate with the same subject as {@link #clientCertificate()} but signed by a CA that this fixture does not trust: the
     * handshake must fail.
     */
    public Path untrustedClientCertificate() {
        return directory.resolve("rogue-client.crt");
    }

    public Path untrustedClientKey() {
        return directory.resolve("rogue-client.key");
    }

    /**
     * The node settings that point the plugin to this fixture, with the identity that Cloud would inject and the client certificate
     * and key of {@link #clientCertificate()}. A test adds what it needs (for example {@code enabled} or a short {@code period}).
     */
    public Settings.Builder pluginSettings() {
        return Settings.builder()
            // the setting is the whole endpoint: the publisher posts to it as it is
            .put(UsageReportingSettings.URL.getKey(), url() + PATH)
            .put(UsageReportingSettings.DEPLOYMENT_ID.getKey(), "fake-deployment")
            .put(UsageReportingSettings.CLUSTER_ID.getKey(), "fake-cluster")
            .put(UsageReportingSettings.PROVIDER.getKey(), "aws")
            .put(UsageReportingSettings.REGION.getKey(), "eu-west-1")
            .put(UsageReportingSettings.SSL_PREFIX + ".certificate", clientCertificate().toString())
            .put(UsageReportingSettings.SSL_PREFIX + ".key", clientKey().toString())
            .putList(UsageReportingSettings.SSL_PREFIX + ".certificate_authorities", certificateAuthority().toString());
    }

    /** Sets the behavior for every request that no queued one-shot behavior takes. The default is {@link Behavior#CREATED}. */
    public void setBehavior(Behavior behavior) {
        this.behavior = Objects.requireNonNull(behavior);
    }

    /** Queues behaviors that answer the next requests, one each, in order, before the default behavior applies again. */
    public void enqueue(Behavior... behaviors) {
        Collections.addAll(oneShot, behaviors);
    }

    /** Every request received so far, oldest first. */
    public List<ReceivedRequest> requests() {
        return List.copyOf(requests);
    }

    /** The ids of every record of every request, in the order received; a resent record appears once per request. */
    public List<String> receivedRecordIds() {
        List<String> ids = new ArrayList<>();
        requests.forEach(r -> ids.addAll(r.recordIds()));
        return ids;
    }

    /** The ids of the records that the fixture stored, once per request that stored them: a resent record appears again. */
    public List<String> acceptedRecordIds() {
        return List.copyOf(acceptedRecordIds);
    }

    /** The first request that satisfies the predicate, or {@code null}. */
    public ReceivedRequest firstRequest(Predicate<ReceivedRequest> predicate) {
        return requests.stream().filter(predicate).findFirst().orElse(null);
    }

    /** Back to a new fixture: forgets the recorded requests and the queued one-shot behaviors, and answers {@link Behavior#CREATED}. */
    public void reset() {
        clearRequests();
        oneShot.clear();
        behavior = Behavior.CREATED;
    }

    /** Forgets the recorded requests, not the script. */
    public void clearRequests() {
        requests.clear();
        acceptedRecordIds.clear();
    }

    @Override
    public void close() {
        server.stop(0);
        // interrupts a handler that is sleeping for a slow behavior, so that a test does not wait for the whole delay
        executor.shutdownNow();
        try {
            if (executor.awaitTermination(10, TimeUnit.SECONDS) == false) {
                executor.shutdownNow();
                throw new IllegalStateException("the fake Usage API did not stop in time");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            ReceivedRequest request = parse(exchange, body);
            requests.add(request);

            if (PATH.equals(request.path()) == false) {
                respond(exchange, 404);
                return;
            }
            if ("POST".equals(request.method()) == false) {
                respond(exchange, 405);
                return;
            }
            Behavior next = oneShot.poll();
            Reply reply = (next != null ? next : behavior).reply(request);
            if (reply.delay().millis() > 0) {
                sleep(reply.delay());
            }
            if (reply.commit()) {
                acceptedRecordIds.addAll(request.recordIds());
            }
            if (reply.dropConnection()) {
                // closing the exchange before any header is sent closes the connection: the client sees it end, not a status
                return;
            }
            respond(exchange, reply.status());
        }
    }

    private static ReceivedRequest parse(HttpExchange exchange, String body) {
        String subject = null;
        if (exchange instanceof HttpsExchange https) {
            try {
                Principal principal = https.getSSLSession().getPeerPrincipal();
                subject = principal.getName();
            } catch (SSLPeerUnverifiedException e) {
                // the server requires a client certificate, so this does not happen; recorded as no subject if it does
            }
        }
        List<Map<String, Object>> records = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, body)) {
            for (Object element : parser.list()) {
                if (element instanceof Map<?, ?> map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> record = (Map<String, Object>) map;
                    records.add(record);
                    ids.add(String.valueOf(record.get("id")));
                } else {
                    throw new IllegalArgumentException("record is not an object");
                }
            }
        } catch (IOException | IllegalArgumentException e) {
            // XContentParseException is an IllegalArgumentException: any body that is not an array of objects has no ids
            records.clear();
            ids.clear();
        }
        return new ReceivedRequest(
            subject,
            exchange.getRequestMethod(),
            exchange.getRequestURI().getPath(),
            body,
            List.copyOf(ids),
            List.copyOf(records)
        );
    }

    private static void respond(HttpExchange exchange, int status) throws IOException {
        byte[] body = ("{\"status\":" + status + "}").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static void sleep(TimeValue delay) {
        try {
            Thread.sleep(delay.millis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Path resource(String name) {
        URL url = FakeUsageApi.class.getResource(RESOURCE_BASE + name);
        if (url == null) {
            throw new IllegalStateException("cannot find the test PEM resource " + RESOURCE_BASE + name);
        }
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("cannot resolve the test PEM resource " + url, e);
        }
    }

    private static SSLContext serverSslContext(Path directory) throws Exception {
        var keyManager = new PemKeyConfig(
            directory.resolve("server.crt").toString(),
            directory.resolve("server.key").toString(),
            new char[0],
            directory
        ).createKeyManager();
        var trustManager = new PemTrustConfig(List.of(directory.resolve("ca.crt").toString()), directory).createTrustManager();
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(new KeyManager[] { keyManager }, new TrustManager[] { trustManager }, null);
        return context;
    }

    /** Makes the server demand a client certificate, so that a client without a trusted one fails the handshake. */
    @SuppressForbidden(reason = "use the JDK https server as a controllable Usage API fixture")
    private static final class RequireClientCertificate extends HttpsConfigurator {
        RequireClientCertificate(SSLContext sslContext) {
            super(sslContext);
        }

        @Override
        public void configure(HttpsParameters params) {
            SSLParameters parameters = getSSLContext().getDefaultSSLParameters();
            parameters.setNeedClientAuth(true);
            params.setSSLParameters(parameters);
        }
    }
}
