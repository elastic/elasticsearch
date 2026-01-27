/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AliyunStsClientTests extends ESTestCase {
    public void testVerifyParsesNestedResponse() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicReference<String> queryRef = new AtomicReference<>();
        server.createContext("/", exchange -> {
            queryRef.set(exchange.getRequestURI().getRawQuery());
            String body = """
                {
                  "GetCallerIdentityResponse": {
                    "Arn": "acs:ram::123:user/test",
                    "AccountId": "123",
                    "UserId": "user-id"
                  }
                }
                """;
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        server.start();
        try {
            AliyunStsClient client = new AliyunStsClient(configForEndpoint(endpoint(server)));
            CloudIamToken token = CloudIamToken.fromHeaders(
                buildSignedHeader("AKID", "sig", "nonce", Instant.now().truncatedTo(ChronoUnit.SECONDS), "sts-token"),
                8192
            );
            PlainActionFuture<IamPrincipal> future = new PlainActionFuture<>();
            client.verify(token, future);
            IamPrincipal principal = future.actionGet();
            assertThat(principal.arn(), is("acs:ram::123:user/test"));
            assertThat(principal.accountId(), is("123"));
            assertThat(principal.userId(), is("user-id"));
            assertThat(principal.principalType(), is(IamPrincipal.PrincipalType.USER));

            Map<String, String> params = parseQuery(queryRef.get());
            assertThat(params.get("Action"), is("GetCallerIdentity"));
            assertThat(params.get("AccessKeyId"), is("AKID"));
            assertThat(params.get("Signature"), is("sig"));
            assertThat(params.get("SecurityToken"), is("sts-token"));
        } finally {
            server.stop(0);
        }
    }

    public void testVerifyParsesTopLevelResponse() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String body = """
                {
                  "Arn": "acs:ram::456:user/demo",
                  "AccountId": "456",
                  "UserId": "demo-id"
                }
                """;
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        server.start();
        try {
            AliyunStsClient client = new AliyunStsClient(configForEndpoint(endpoint(server)));
            CloudIamToken token = CloudIamToken.fromHeaders(
                buildSignedHeader("AKID", "sig", "nonce", Instant.now().truncatedTo(ChronoUnit.SECONDS), null),
                8192
            );
            PlainActionFuture<IamPrincipal> future = new PlainActionFuture<>();
            client.verify(token, future);
            IamPrincipal principal = future.actionGet();
            assertThat(principal.arn(), is("acs:ram::456:user/demo"));
            assertThat(principal.accountId(), is("456"));
        } finally {
            server.stop(0);
        }
    }

    public void testRejectsUnknownParameter() {
        AliyunStsClient client = new AliyunStsClient(configForEndpoint("http://127.0.0.1:9"));
        String json = """
            {
              "Action": "GetCallerIdentity",
              "Version": "2015-04-01",
              "AccessKeyId": "AKID",
              "Signature": "sig",
              "SignatureMethod": "HMAC-SHA1",
              "SignatureVersion": "1.0",
              "SignatureNonce": "nonce",
              "Timestamp": "2025-01-01T00:00:00Z",
              "ExtraParam": "nope"
            }
            """;
        String header = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        CloudIamToken badToken = CloudIamToken.fromHeaders(header, 8192);
        PlainActionFuture<IamPrincipal> future = new PlainActionFuture<>();
        client.verify(badToken, future);
        Exception e = expectThrows(Exception.class, future::actionGet);
        assertThat(e.getMessage(), containsString("unsupported signed parameter"));
    }

    public void testRejectsUnsupportedSignatureMethod() {
        AliyunStsClient client = new AliyunStsClient(configForEndpoint("http://127.0.0.1:9"));
        String json = """
            {
              "Action": "GetCallerIdentity",
              "Version": "2015-04-01",
              "AccessKeyId": "AKID",
              "Signature": "sig",
              "SignatureMethod": "HMAC-SHA512",
              "SignatureVersion": "1.0",
              "SignatureNonce": "nonce",
              "Timestamp": "2025-01-01T00:00:00Z"
            }
            """;
        String header = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        CloudIamToken token = CloudIamToken.fromHeaders(header, 8192);
        PlainActionFuture<IamPrincipal> future = new PlainActionFuture<>();
        client.verify(token, future);
        Exception e = expectThrows(Exception.class, future::actionGet);
        assertThat(e.getMessage(), containsString("unsupported signature method"));
    }

    private RealmConfig configForEndpoint(String endpoint) {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(RealmSettings.realmSettingPrefix(CloudIamRealmSettings.TYPE) + "iam1.order", 0)
            .put(CloudIamRealmSettings.IAM_ENDPOINT.getConcreteSettingForNamespace("iam1").getKey(), endpoint)
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        RealmConfig.RealmIdentifier id = new RealmConfig.RealmIdentifier(CloudIamRealmSettings.TYPE, "iam1");
        return new RealmConfig(id, settings, env, new ThreadContext(settings));
    }

    private String endpoint(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isEmpty()) {
            return params;
        }
        for (String pair : query.split("&")) {
            int idx = pair.indexOf('=');
            if (idx <= 0) {
                continue;
            }
            String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
            String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
            params.put(key, value);
        }
        return params;
    }

    private String buildSignedHeader(String accessKeyId, String signature, String nonce, Instant timestamp, String sessionToken) {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
            .append("\"Action\":\"GetCallerIdentity\",")
            .append("\"Version\":\"2015-04-01\",")
            .append("\"AccessKeyId\":\"").append(accessKeyId).append("\",")
            .append("\"Signature\":\"").append(signature).append("\",")
            .append("\"SignatureMethod\":\"HMAC-SHA1\",")
            .append("\"SignatureVersion\":\"1.0\",")
            .append("\"SignatureNonce\":\"").append(nonce).append("\",")
            .append("\"Timestamp\":\"").append(timestamp.toString()).append("\"");
        if (sessionToken != null) {
            builder.append(",\"SecurityToken\":\"").append(sessionToken).append("\"");
        }
        builder.append("}");
        return Base64.getEncoder().encodeToString(builder.toString().getBytes(StandardCharsets.UTF_8));
    }
}
