/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage.testfixture;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.common.ssl.PemTrustConfig;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.usage.UsageReportingSettings;
import org.elasticsearch.xpack.esql.usage.testfixture.FakeUsageApi.Behavior;
import org.elasticsearch.xpack.esql.usage.testfixture.FakeUsageApi.ReceivedRequest;
import org.elasticsearch.xpack.esql.usage.testfixture.FakeUsageApi.Reply;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests the fixture, so that a failure of a test that uses it points to the code under test and not to the fixture: the handshake
 * rules, what is recorded, and each scripted behavior.
 */
public class FakeUsageApiTests extends ESTestCase {

    private static final String TWO_RECORDS = "[{\"id\":\"r1\",\"usage\":{\"quantity\":5}},{\"id\":\"r2\",\"usage\":{\"quantity\":7}}]";

    private FakeUsageApi api;

    @Before
    public void startApi() throws Exception {
        api = FakeUsageApi.start();
    }

    @After
    public void stopApi() {
        api.close();
    }

    /** A client with the given identity, or with none; it trusts the CA of the fixture, as the plugin does through its settings. */
    private HttpClient client(Path certificate, Path key) throws Exception {
        KeyManager[] keyManagers = certificate == null
            ? null
            : new KeyManager[] {
                new PemKeyConfig(certificate.toString(), key.toString(), new char[0], certificate.getParent()).createKeyManager() };
        var trust = new PemTrustConfig(List.of(api.certificateAuthority().toString()), api.certificateAuthority().getParent())
            .createTrustManager();
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers, new TrustManager[] { trust }, null);
        return HttpClient.newBuilder().sslContext(context).connectTimeout(Duration.ofSeconds(10)).build();
    }

    private HttpClient validClient() throws Exception {
        return client(api.clientCertificate(), api.clientKey());
    }

    private HttpResponse<String> post(HttpClient client, String body) throws Exception {
        return post(client, FakeUsageApi.PATH, body, Duration.ofSeconds(30));
    }

    private HttpResponse<String> post(HttpClient client, String path, String body, Duration timeout) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(api.url() + path))
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public void testHandshakeSucceedsWithTheRightClientCertificate() throws Exception {
        try (HttpClient client = validClient()) {
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(201));
        }
        assertThat(api.requests(), hasSize(1));
    }

    public void testHandshakeFailsWithoutClientCertificate() throws Exception {
        try (HttpClient client = client(null, null)) {
            expectThrows(IOException.class, () -> post(client, TWO_RECORDS));
        }
        assertThat("a client that fails the handshake never reaches the handler", api.requests(), empty());
    }

    public void testHandshakeFailsWithACertificateOfAnUntrustedAuthority() throws Exception {
        try (HttpClient client = client(api.untrustedClientCertificate(), api.untrustedClientKey())) {
            expectThrows(IOException.class, () -> post(client, TWO_RECORDS));
        }
        assertThat(api.requests(), empty());
    }

    public void testRecordsTheClientIdentityTheBodyAndTheRecordIds() throws Exception {
        try (HttpClient client = validClient()) {
            post(client, TWO_RECORDS);
        }
        ReceivedRequest request = api.requests().get(0);
        assertThat(request.clientSubject(), equalTo(FakeUsageApi.CLIENT_SUBJECT));
        assertThat(request.method(), equalTo("POST"));
        assertThat(request.path(), equalTo(FakeUsageApi.PATH));
        assertThat(request.body(), equalTo(TWO_RECORDS));
        assertThat(request.recordIds(), equalTo(List.of("r1", "r2")));
        assertThat(request.records().get(1).get("usage"), equalTo(Map.of("quantity", 7)));
        assertThat(api.receivedRecordIds(), equalTo(List.of("r1", "r2")));
        assertThat(api.acceptedRecordIds(), equalTo(List.of("r1", "r2")));
    }

    public void testTheOtherClientCertificateHasItsOwnSubject() throws Exception {
        try (HttpClient client = client(api.otherClientCertificate(), api.otherClientKey())) {
            post(client, TWO_RECORDS);
        }
        assertThat(api.requests().get(0).clientSubject(), equalTo(FakeUsageApi.OTHER_CLIENT_SUBJECT));
    }

    public void testAnEmptyOrMalformedBodyIsRecordedWithoutIds() throws Exception {
        try (HttpClient client = validClient()) {
            assertThat(post(client, "[]").statusCode(), equalTo(201));
            post(client, "not json");
            post(client, "[1, 2]");
        }
        assertThat(api.requests(), hasSize(3));
        api.requests().forEach(r -> assertThat(r.recordIds(), empty()));
        assertThat(api.requests().get(1).body(), equalTo("not json"));
    }

    public void testScriptedStatuses() throws Exception {
        try (HttpClient client = validClient()) {
            for (int status : new int[] { 400, 403, 408, 429, 503 }) {
                api.setBehavior(Behavior.always(status));
                assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(status));
            }
        }
        assertThat(api.acceptedRecordIds(), empty());
        assertThat(api.requests(), hasSize(5));
    }

    public void testRejectRecordFailsOnlyTheBatchesThatContainIt() throws Exception {
        api.setBehavior(Behavior.rejectRecord("r2"));
        try (HttpClient client = validClient()) {
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(400));
            assertThat(post(client, "[{\"id\":\"r1\"}]").statusCode(), equalTo(201));
            assertThat(post(client, "[{\"id\":\"r2\"}]").statusCode(), equalTo(400));
        }
        assertThat(api.acceptedRecordIds(), equalTo(List.of("r1")));
        assertThat(api.receivedRecordIds(), equalTo(List.of("r1", "r2", "r1", "r2")));
    }

    public void testOneShotBehaviorsRunInOrderThenTheDefaultApplies() throws Exception {
        api.enqueue(Behavior.always(503), Behavior.always(429));
        try (HttpClient client = validClient()) {
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(503));
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(429));
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(201));
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(201));
        }
        assertThat("a resent record is recorded once per request", api.receivedRecordIds(), hasSize(8));
        assertThat(api.acceptedRecordIds(), equalTo(List.of("r1", "r2", "r1", "r2")));
    }

    public void testAcceptOnlyAGivenClientCertificate() throws Exception {
        api.setBehavior(Behavior.acceptOnlyClient(FakeUsageApi.CLIENT_SUBJECT));
        try (HttpClient valid = validClient(); HttpClient other = client(api.otherClientCertificate(), api.otherClientKey())) {
            assertThat(post(valid, TWO_RECORDS).statusCode(), equalTo(201));
            assertThat(post(other, TWO_RECORDS).statusCode(), equalTo(403));
        }
    }

    public void testSlowResponseTimesOutTheClient() throws Exception {
        api.setBehavior(Behavior.slow(TimeValue.timeValueSeconds(30)));
        try (HttpClient client = validClient()) {
            // the timeout covers the handshake too, so it must not be so short that the request is not even received
            expectThrows(HttpTimeoutException.class, () -> post(client, FakeUsageApi.PATH, TWO_RECORDS, Duration.ofSeconds(5)));
        }
        assertBusy(() -> assertThat("the request is recorded before the fixture waits", api.requests(), hasSize(1)));
    }

    public void testSlowResponseIsAnsweredWhenTheClientWaits() throws Exception {
        api.setBehavior(Behavior.slow(TimeValue.timeValueMillis(300)));
        try (HttpClient client = validClient()) {
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(201));
        }
    }

    public void testDroppedConnectionIsANetworkFailureAndTheServerKeepsServing() throws Exception {
        api.enqueue(Behavior.dropConnection());
        try (HttpClient client = validClient()) {
            expectThrows(IOException.class, () -> post(client, TWO_RECORDS));
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(201));
        }
        assertThat(api.requests(), hasSize(2));
        assertThat("only the answered request is accepted", api.acceptedRecordIds(), equalTo(List.of("r1", "r2")));
    }

    public void testAcceptThenDropStoresTheRecordsAlthoughTheClientSeesAFailure() throws Exception {
        api.enqueue(Behavior.acceptThenDropConnection());
        try (HttpClient client = validClient()) {
            expectThrows(IOException.class, () -> post(client, TWO_RECORDS));
            assertThat("the client does not know that the batch is stored", api.acceptedRecordIds(), equalTo(List.of("r1", "r2")));
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(201));
        }
        assertThat("the retry is a duplicate of the stored records", api.acceptedRecordIds(), equalTo(List.of("r1", "r2", "r1", "r2")));
    }

    public void testRepliesThatTheFixtureCannotSendAreRejected() {
        expectThrows(IllegalArgumentException.class, () -> Reply.status(0));
        expectThrows(IllegalArgumentException.class, () -> Reply.status(204));
        expectThrows(IllegalArgumentException.class, () -> Reply.status(100));
    }

    public void testOtherPathsAndMethods() throws Exception {
        try (HttpClient client = validClient()) {
            assertThat(post(client, "/api/v1/other", TWO_RECORDS, Duration.ofSeconds(30)).statusCode(), equalTo(404));
            HttpRequest get = HttpRequest.newBuilder(URI.create(api.url() + FakeUsageApi.PATH)).GET().build();
            assertThat(client.send(get, HttpResponse.BodyHandlers.ofString()).statusCode(), equalTo(405));
        }
        assertThat(api.acceptedRecordIds(), empty());
    }

    public void testClearRequestsKeepsTheScript() throws Exception {
        api.setBehavior(Behavior.always(503));
        try (HttpClient client = validClient()) {
            post(client, TWO_RECORDS);
            api.clearRequests();
            assertThat(api.requests(), empty());
            assertThat(post(client, TWO_RECORDS).statusCode(), equalTo(503));
        }
        assertThat(api.firstRequest(r -> r.recordIds().contains("r2")), notNullValue());
        assertThat(api.firstRequest(r -> r.recordIds().contains("nope")), nullValue());
    }

    public void testPluginSettingsPointToTheFixtureAndAreComplete() throws Exception {
        Settings settings = api.pluginSettings().put(UsageReportingSettings.ENABLED.getKey(), true).build();
        assertThat(UsageReportingSettings.URL.get(settings), startsWith("https://"));
        assertThat(UsageReportingSettings.of(settings).problems(), empty());
        assertThat(UsageReportingSettings.URL.get(settings), equalTo(api.url() + FakeUsageApi.PATH));
        assertThat(settings.get(UsageReportingSettings.SSL_PREFIX + ".certificate"), containsString("client.crt"));
        assertThat(settings.get(UsageReportingSettings.SSL_PREFIX + ".key"), containsString("client.key"));
        assertThat(UsageReportingSettings.CLUSTER_ID.get(settings), equalTo("fake-cluster"));
    }
}
