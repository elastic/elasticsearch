/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.elasticsearch.xpack.core.security.HttpResponse;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ExternalEnrollmentTokenGenerator extends BaseEnrollmentTokenGenerator {
    protected static final String ENROLL_API_KEY_EXPIRATION = "30m";

    private static final Logger logger = LogManager.getLogger(ExternalEnrollmentTokenGenerator.class);
    private final Environment environment;
    private final SSLService sslService;
    private final CommandLineHttpClient client;

    public ExternalEnrollmentTokenGenerator(Environment environment) throws MalformedURLException {
        this(environment, new CommandLineHttpClient(environment));
    }

    // protected for testing
    protected ExternalEnrollmentTokenGenerator(Environment environment, CommandLineHttpClient client) {
        this.environment = environment;
        this.sslService = new SSLService(environment);
        this.client = client;
    }

    public EnrollmentToken createNodeEnrollmentToken(String user, SecureString password, URL baseUrl) throws Exception {
        return this.create(user, password, NodeEnrollmentAction.NAME, baseUrl);
    }

    public EnrollmentToken createKibanaEnrollmentToken(String user, SecureString password, URL baseUrl) throws Exception {
        return this.create(user, password, KibanaEnrollmentAction.NAME, baseUrl);
    }

    protected EnrollmentToken create(String user, SecureString password, String action, URL baseUrl) throws Exception {
        if (XPackSettings.ENROLLMENT_ENABLED.get(environment.settings()) != true) {
            throw new IllegalStateException("[xpack.security.enrollment.enabled] must be set to `true` to create an enrollment token");
        }
        final String fingerprint = getHttpsCaFingerprint(sslService);
        final String apiKey = getApiKeyCredentials(user, password, action, baseUrl);
        final Tuple<List<String>, String> httpInfo = getNodeInfo(user, password, baseUrl);
        return new EnrollmentToken(apiKey, fingerprint, httpInfo.v2(), httpInfo.v1());
    }

    private static HttpResponse.HttpResponseBuilder responseBuilder(InputStream is) throws IOException {
        final HttpResponse.HttpResponseBuilder httpResponseBuilder = new HttpResponse.HttpResponseBuilder();
        if (is != null) {
            String responseBody = Streams.readFully(is).utf8ToString();
            logger.debug(responseBody);
            httpResponseBuilder.withResponseBody(responseBody);
        } else {
            logger.debug("Error building http response body: null response");
        }
        return httpResponseBuilder;
    }

    protected static URL createAPIKeyUrl(URL baseUrl) throws MalformedURLException, URISyntaxException {
        return new URL(baseUrl, (baseUrl.toURI().getPath() + "/_security/api_key").replaceAll("/+", "/"));
    }

    protected static URL getHttpInfoUrl(URL baseUrl) throws MalformedURLException, URISyntaxException {
        return new URL(baseUrl, (baseUrl.toURI().getPath() + "/_nodes/_local/http").replaceAll("/+", "/"));
    }

    @SuppressWarnings("unchecked")
    protected static List<String> getBoundAddresses(Map<?, ?> nodesInfo) {
        nodesInfo = (Map<?, ?>) nodesInfo.get("nodes");
        Map<?, ?> nodeInfo = (Map<?, ?>) nodesInfo.values().iterator().next();
        Map<?, ?> http = (Map<?, ?>) nodeInfo.get("http");
        final List<String> addresses = new ArrayList<>();
        addresses.addAll((Collection<? extends String>) http.get("bound_address"));
        addresses.add(getIpFromPublishAddress((String) http.get("publish_address")));
        return addresses;
    }

    static String getVersion(Map<?, ?> nodesInfo) {
        nodesInfo = (Map<?, ?>) nodesInfo.get("nodes");
        Map<?, ?> nodeInfo = (Map<?, ?>) nodesInfo.values().iterator().next();
        return nodeInfo.get("version").toString();
    }

    protected String getApiKeyCredentials(String user, SecureString password, String action, URL baseUrl) throws Exception {
        final CheckedSupplier<String, Exception> createApiKeyRequestBodySupplier = () -> {
            XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
            xContentBuilder.startObject()
                .field("name", "enrollment_token_API_key_" + UUIDs.base64UUID())
                .field("expiration", ENROLL_API_KEY_EXPIRATION)
                .startObject("role_descriptors")
                .startObject("create_enrollment_token")
                .array("cluster", action)
                .endObject()
                .endObject()
                .endObject();
            return Strings.toString(xContentBuilder);
        };

        final URL createApiKeyUrl = createAPIKeyUrl(baseUrl);
        final HttpResponse httpResponseApiKey = client.execute(
            "POST",
            createApiKeyUrl,
            user,
            password,
            createApiKeyRequestBodySupplier,
            ExternalEnrollmentTokenGenerator::responseBuilder
        );
        final int httpCode = httpResponseApiKey.getHttpStatus();

        if (httpCode != HttpURLConnection.HTTP_OK) {
            logger.error(
                "Error " + httpCode + "when calling GET " + createApiKeyUrl + ". ResponseBody: " + httpResponseApiKey.getResponseBody()
            );
            throw new IllegalStateException("Unexpected response code [" + httpCode + "] from calling POST " + createApiKeyUrl);
        }

        final String apiKey = Objects.toString(httpResponseApiKey.getResponseBody().get("api_key"), "");
        final String apiId = Objects.toString(httpResponseApiKey.getResponseBody().get("id"), "");
        if (Strings.isNullOrEmpty(apiKey) || Strings.isNullOrEmpty(apiId)) {
            throw new IllegalStateException("Could not create an api key.");
        }
        return apiId + ":" + apiKey;
    }

    protected Tuple<List<String>, String> getNodeInfo(String user, SecureString password, URL baseUrl) throws Exception {
        final URL httpInfoUrl = getHttpInfoUrl(baseUrl);
        final HttpResponse httpResponseHttp = client.execute("GET", httpInfoUrl, user, password, () -> null, is -> responseBuilder(is));
        final int httpCode = httpResponseHttp.getHttpStatus();

        if (httpCode != HttpURLConnection.HTTP_OK) {
            logger.error("Error " + httpCode + "when calling GET " + httpInfoUrl + ". ResponseBody: " + httpResponseHttp.getResponseBody());
            throw new IllegalStateException("Unexpected response code [" + httpCode + "] from calling GET " + httpInfoUrl);
        }

        final List<String> addresses = getBoundAddresses(httpResponseHttp.getResponseBody());
        if (addresses == null || addresses.isEmpty()) {
            logger.error(
                "No bound addresses found in response from calling GET "
                    + httpInfoUrl
                    + ". ResponseBody: "
                    + httpResponseHttp.getResponseBody()
            );
            throw new IllegalStateException("No bound addresses found in response from calling GET " + httpInfoUrl);
        }
        final List<String> filteredAddresses = getFilteredAddresses(addresses);

        final String stackVersion = getVersion(httpResponseHttp.getResponseBody());
        if (stackVersion == null || stackVersion.isEmpty()) {
            throw new IllegalStateException("Could not retrieve the version.");
        }
        return new Tuple<>(filteredAddresses, stackVersion);
    }

}
