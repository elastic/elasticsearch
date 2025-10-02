/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.IsNot.not;

public class SecuritySlowLogIT extends ESRestTestCase {

    private record TestIndexData(
        String name,
        boolean searchSlowLogEnabled,
        boolean indexSlowLogEnabled,
        boolean searchSlowLogUserEnabled,
        boolean indexSlowLogUserEnabled
    ) {}

    private static int currentSearchLogIndex = 0;
    private static int currentIndexLogIndex = 0;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .user("admin_user", "admin-password")
        .user("api_user", "api-password", "superuser", false)
        .build();

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("api_user", new SecureString("api-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testSlowLogWithApiUser() throws Exception {
        List<TestIndexData> testIndices = randomTestIndexData();
        for (TestIndexData testData : testIndices) {
            searchSomeData(testData.name);
            indexSomeData(testData.name);
        }

        Map<String, Object> expectedUser = Map.of("user.name", "api_user", "user.realm", "default_file", "auth.type", "REALM");

        verifySearchSlowLogMatchesTestData(testIndices, expectedUser);
        verifyIndexSlowLogMatchesTestData(testIndices, expectedUser);
    }

    public void testSlowLogWithUserWithFullName() throws Exception {
        List<TestIndexData> testIndices = randomTestIndexData();
        createUserWithFullName("full_name", "full-name-password", "Full Name", new String[] { "superuser" });
        for (TestIndexData testData : testIndices) {
            final RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue("full_name", new SecureString("full-name-password".toCharArray())))
                .build();
            searchSomeData(testData.name, requestOptions);
            indexSomeData(testData.name, requestOptions);
        }

        Map<String, Object> expectedUser = Map.of(
            "user.name",
            "full_name",
            "user.full_name",
            "Full Name",
            "user.realm",
            "default_native",
            "auth.type",
            "REALM"
        );

        verifySearchSlowLogMatchesTestData(testIndices, expectedUser);
        verifyIndexSlowLogMatchesTestData(testIndices, expectedUser);
    }

    public void testSlowLogWithUserWithFullNameWithRunAs() throws Exception {
        List<TestIndexData> testIndices = randomTestIndexData();
        createUserWithFullName("full_name", "full-name-password", "Full Name", new String[] { "superuser" });
        for (TestIndexData testData : testIndices) {
            final RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
                .addHeader("es-security-runas-user", "full_name")
                .addHeader("Authorization", basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray())))
                .build();
            searchSomeData(testData.name, requestOptions);
            indexSomeData(testData.name, requestOptions);
        }

        Map<String, Object> expectedUser = Map.of(
            "user.name",
            "admin_user",
            "user.effective.full_name",
            "Full Name",
            "user.realm",
            "default_file",
            "auth.type",
            "REALM"
        );

        verifySearchSlowLogMatchesTestData(testIndices, expectedUser);
        verifyIndexSlowLogMatchesTestData(testIndices, expectedUser);
    }

    public void testSlowLogWithApiKey() throws Exception {
        List<TestIndexData> testIndices = randomTestIndexData();
        String apiKeyName = randomAlphaOfLengthBetween(10, 15);
        Map<String, Object> createApiKeyResponse = createApiKey(
            apiKeyName,
            basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()))
        );
        String apiKeyHeader = Base64.getEncoder()
            .encodeToString(
                (createApiKeyResponse.get("id") + ":" + createApiKeyResponse.get("api_key").toString()).getBytes(StandardCharsets.UTF_8)
            );

        for (TestIndexData testData : testIndices) {
            final RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", "ApiKey " + apiKeyHeader)
                .build();
            searchSomeData(testData.name, requestOptions);
            indexSomeData(testData.name, requestOptions);
        }

        Map<String, Object> expectedUser = Map.of(
            "user.name",
            "admin_user",
            "user.realm",
            "_es_api_key",
            "auth.type",
            "API_KEY",
            "apikey.id",
            createApiKeyResponse.get("id"),
            "apikey.name",
            apiKeyName
        );

        verifySearchSlowLogMatchesTestData(testIndices, expectedUser);
        verifyIndexSlowLogMatchesTestData(testIndices, expectedUser);
    }

    public void testSlowLogWithRunAs() throws Exception {
        List<TestIndexData> testIndices = randomTestIndexData();

        for (TestIndexData testData : testIndices) {
            final RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
                .addHeader("es-security-runas-user", "api_user")
                .addHeader("Authorization", basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray())))
                .build();
            searchSomeData(testData.name, requestOptions);
            indexSomeData(testData.name, requestOptions);
        }

        Map<String, Object> expectedUser = Map.of(
            "user.name",
            "admin_user",
            "user.effective.name",
            "api_user",
            "user.realm",
            "default_file",
            "user.effective.realm",
            "default_file",
            "auth.type",
            "REALM"
        );

        verifySearchSlowLogMatchesTestData(testIndices, expectedUser);
        verifyIndexSlowLogMatchesTestData(testIndices, expectedUser);
    }

    public void testSlowLogWithServiceAccount() throws Exception {
        List<TestIndexData> testIndices = randomTestIndexData();
        Map<String, Object> createServiceAccountResponse = createServiceAccountToken();
        @SuppressWarnings("unchecked")
        String tokenValue = ((Map<String, Object>) createServiceAccountResponse.get("token")).get("value").toString();

        for (TestIndexData testData : testIndices) {
            final RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", "Bearer " + tokenValue)
                .build();
            searchSomeData(testData.name, requestOptions);
            indexSomeData(testData.name, requestOptions);
        }

        Map<String, Object> expectedUser = Map.of(
            "user.name",
            "elastic/fleet-server",
            "user.realm",
            "_service_account",
            "auth.type",
            "TOKEN"
        );

        verifySearchSlowLogMatchesTestData(testIndices, expectedUser);
        verifyIndexSlowLogMatchesTestData(testIndices, expectedUser);
    }

    private static void enableSearchSlowLog(String index, boolean includeUser) throws IOException {
        final Request request = new Request("PUT", "/" + index + "/_settings");
        request.setJsonEntity(
            "{\"index.search.slowlog.threshold.query."
                + randomFrom("trace", "warn", "debug", "info")
                + "\": \"0\", "
                + "\"index.search.slowlog.include.user\": "
                + includeUser
                + "}"
        );
        client().performRequest(request);
    }

    private static void enableIndexingSlowLog(String index, boolean includeUser) throws IOException {
        final Request request = new Request("PUT", "/" + index + "/_settings");
        request.setJsonEntity(
            "{\"index.indexing.slowlog.threshold.index."
                + randomFrom("trace", "warn", "debug", "info")
                + "\": \"0\", "
                + "\"index.indexing.slowlog.include.user\": "
                + includeUser
                + "}"
        );
        client().performRequest(request);
    }

    private static void indexSomeData(String index) throws IOException {
        indexSomeData(index, RequestOptions.DEFAULT.toBuilder().build());
    }

    private static void searchSomeData(String index) throws IOException {
        searchSomeData(index, RequestOptions.DEFAULT.toBuilder().build());
    }

    private static void indexSomeData(String index, RequestOptions requestOptions) throws IOException {
        final Request request = new Request("PUT", "/" + index + "/_doc/1");
        request.setOptions(requestOptions);
        request.setJsonEntity("{ \"foobar\" : true }");
        client().performRequest(request);
    }

    private static void searchSomeData(String index, RequestOptions requestOptions) throws IOException {
        Request request = new Request("GET", "/" + index + "/_search");
        request.setOptions(requestOptions);
        client().performRequest(request);
    }

    private static void setupTestIndex(TestIndexData testIndexData) throws IOException {
        indexSomeData(testIndexData.name);
        if (testIndexData.indexSlowLogEnabled) {
            enableIndexingSlowLog(testIndexData.name, testIndexData.indexSlowLogUserEnabled);
        }
        if (testIndexData.searchSlowLogEnabled) {
            enableSearchSlowLog(testIndexData.name, testIndexData.searchSlowLogUserEnabled);
        }
    }

    private static void createUserWithFullName(String user, String password, String fullName, String[] roles) throws IOException {
        Request request = new Request("POST", "/_security/user/" + user);
        request.setJsonEntity(
            "{ \"full_name\" : \""
                + fullName
                + "\", \"roles\": [\""
                + String.join("\",\"", roles)
                + "\"], \"password\": \""
                + password
                + "\" }"
        );
        Response response = client().performRequest(request);
        assertOK(response);
    }

    private static List<TestIndexData> randomTestIndexData() throws IOException {
        List<TestIndexData> testData = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            TestIndexData randomTestData = new TestIndexData(
                "agentless-" + randomAlphaOfLengthBetween(5, 10).toLowerCase() + "-" + i,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            setupTestIndex(randomTestData);
            testData.add(randomTestData);
        }
        return testData;
    }

    private void verifySearchSlowLogMatchesTestData(List<TestIndexData> testIndices, Map<String, Object> expectedUserData)
        throws Exception {
        verifySlowLog(logLines -> {
            for (TestIndexData testIndex : testIndices) {
                if (testIndex.searchSlowLogEnabled) {
                    Map<String, Object> logLine = logLines.get(currentSearchLogIndex);
                    if (testIndex.searchSlowLogUserEnabled) {
                        assertThat(expectedUserData.entrySet(), everyItem(in(logLine.entrySet())));
                    } else {
                        assertThat(expectedUserData.entrySet(), everyItem(not(in(logLine.entrySet()))));
                    }
                    currentSearchLogIndex++;
                }
            }
        }, LogType.SEARCH_SLOW);
    }

    private void verifyIndexSlowLogMatchesTestData(List<TestIndexData> testIndices, Map<String, Object> expectedUserData) throws Exception {
        verifySlowLog(logLines -> {
            for (TestIndexData testIndex : testIndices) {
                if (testIndex.indexSlowLogEnabled) {
                    Map<String, Object> logLine = logLines.get(currentIndexLogIndex);
                    if (testIndex.indexSlowLogUserEnabled) {
                        assertThat(expectedUserData.entrySet(), everyItem(in(logLine.entrySet())));
                    } else {
                        assertThat(expectedUserData.entrySet(), everyItem(not(in(logLine.entrySet()))));
                    }
                    currentIndexLogIndex++;
                }
            }
        }, LogType.INDEXING_SLOW);
    }

    private static void verifySlowLog(Consumer<List<Map<String, Object>>> logVerifier, LogType logType) throws Exception {
        assertBusy(() -> {
            try (var slowLog = cluster.getNodeLog(0, logType)) {
                final List<String> lines = Streams.readAllLines(slowLog);
                logVerifier.accept(
                    lines.stream().map(line -> XContentHelper.convertToMap(XContentType.JSON.xContent(), line, true)).toList()
                );
            }
        }, 5, TimeUnit.SECONDS);
    }

    private static Map<String, Object> createApiKey(String name, String authHeader) throws IOException {
        final Request request = new Request("POST", "/_security/api_key");

        request.setJsonEntity(Strings.format("""
            {"name":"%s"}""", name));

        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final Response response = client().performRequest(request);
        assertOK(response);
        return responseAsMap(response);
    }

    private static Map<String, Object> createServiceAccountToken() throws IOException {
        final Request createServiceTokenRequest = new Request("POST", "/_security/service/elastic/fleet-server/credential/token");
        final Response createServiceTokenResponse = adminClient().performRequest(createServiceTokenRequest);
        assertOK(createServiceTokenResponse);

        return responseAsMap(createServiceTokenResponse);
    }
}
