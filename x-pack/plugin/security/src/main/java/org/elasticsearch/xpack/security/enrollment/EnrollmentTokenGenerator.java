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
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.ssl.KeyConfig;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.StoreKeyConfig;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class EnrollmentTokenGenerator {
    protected static final String ENROLL_API_KEY_EXPIRATION = "30m";

    private static final Logger logger = LogManager.getLogger(EnrollmentTokenGenerator.class);
    private final Environment environment;
    private final SSLService sslService;
    private final CommandLineHttpClient client;
    private final URL defaultUrl;

    public EnrollmentTokenGenerator(Environment environment) throws MalformedURLException {
        this(environment, new CommandLineHttpClient(environment));
    }

    // protected for testing
    protected EnrollmentTokenGenerator(Environment environment, CommandLineHttpClient client) throws MalformedURLException {
        this.environment = environment;
        this.sslService = new SSLService(environment);
        this.client = client;
        this.defaultUrl = new URL(client.getDefaultURL());
    }

    public EnrollmentToken createNodeEnrollmentToken(String user, SecureString password) throws Exception {
        return this.create(user, password, NodeEnrollmentAction.NAME);
    }

    public EnrollmentToken createKibanaEnrollmentToken(String user, SecureString password) throws Exception {
        return this.create(user, password, KibanaEnrollmentAction.NAME);
    }

    protected EnrollmentToken create(String user, SecureString password, String action) throws Exception {
        if (XPackSettings.ENROLLMENT_ENABLED.get(environment.settings()) != true) {
            throw new IllegalStateException("[xpack.security.enrollment.enabled] must be set to `true` to create an enrollment token");
        }
        final String fingerprint = getCaFingerprint();
        final String apiKey = getApiKeyCredentials(user, password, action);
        final Tuple<List<String>, String> httpInfo = getNodeInfo(user, password);
        return new EnrollmentToken(apiKey, fingerprint, httpInfo.v2(), httpInfo.v1());
    }

    private HttpResponse.HttpResponseBuilder responseBuilder(InputStream is) throws IOException {
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

    protected URL createAPIKeyUrl() throws MalformedURLException, URISyntaxException {
        return new URL(defaultUrl, (defaultUrl.toURI().getPath() + "/_security/api_key").replaceAll("/+", "/"));
    }

    protected URL getHttpInfoUrl() throws MalformedURLException, URISyntaxException {
        return new URL(defaultUrl, (defaultUrl.toURI().getPath() + "/_nodes/_local/http").replaceAll("/+", "/"));
    }

    @SuppressWarnings("unchecked")
    protected static List<String> getBoundAddresses(Map<?, ?> nodesInfo) {
        nodesInfo = (Map<?, ?>) nodesInfo.get("nodes");
        Map<?, ?> nodeInfo = (Map<?, ?>) nodesInfo.values().iterator().next();
        Map<?, ?> http = (Map<?, ?>) nodeInfo.get("http");
        return (ArrayList<String>) http.get("bound_address");
    }

    static String getVersion(Map<?, ?> nodesInfo) {
        nodesInfo = (Map<?, ?>) nodesInfo.get("nodes");
        Map<?, ?> nodeInfo = (Map<?, ?>) nodesInfo.values().iterator().next();
        return nodeInfo.get("version").toString();
    }

    protected String getApiKeyCredentials(String user, SecureString password, String action) throws Exception {
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

        final URL createApiKeyUrl = createAPIKeyUrl();
        final HttpResponse httpResponseApiKey = client.execute("POST", createApiKeyUrl, user, password,
            createApiKeyRequestBodySupplier, is -> responseBuilder(is));
        final int httpCode = httpResponseApiKey.getHttpStatus();

        if (httpCode != HttpURLConnection.HTTP_OK) {
            logger.error("Error " + httpCode + "when calling GET " + createApiKeyUrl + ". ResponseBody: " +
                httpResponseApiKey.getResponseBody());
            throw new IllegalStateException("Unexpected response code [" + httpCode + "] from calling POST "
                + createApiKeyUrl);
        }

        final String apiKey = Objects.toString(httpResponseApiKey.getResponseBody().get("api_key"), "");
        final String apiId = Objects.toString(httpResponseApiKey.getResponseBody().get("id"), "");
        if (Strings.isNullOrEmpty(apiKey) || Strings.isNullOrEmpty(apiId)) {
            throw new IllegalStateException("Could not create an api key.");
        }
        return apiId + ":" + apiKey;
    }

    protected Tuple<List<String>, String> getNodeInfo(String user, SecureString password) throws Exception {
        final URL httpInfoUrl = getHttpInfoUrl();
        final HttpResponse httpResponseHttp = client.execute("GET", httpInfoUrl, user, password, () -> null, is -> responseBuilder(is));
        final int httpCode = httpResponseHttp.getHttpStatus();

        if (httpCode != HttpURLConnection.HTTP_OK) {
            logger.error("Error " + httpCode + "when calling GET " + httpInfoUrl + ". ResponseBody: " +
                httpResponseHttp.getResponseBody());
            throw new IllegalStateException("Unexpected response code [" + httpCode + "] from calling GET " + httpInfoUrl);
        }

        final List<String> addresses = getBoundAddresses(httpResponseHttp.getResponseBody());
        if (addresses == null || addresses.isEmpty()) {
            logger.error("No bound addresses found in response from calling GET " + httpInfoUrl + ". ResponseBody: " +
                httpResponseHttp.getResponseBody());
            throw new IllegalStateException("No bound addresses found in response from calling GET " + httpInfoUrl);
        }
        final List<String> filtered_addresses = getFilteredAddresses(addresses);

        final String stackVersion = getVersion(httpResponseHttp.getResponseBody());
        if (stackVersion == null || stackVersion.isEmpty()) {
            throw new IllegalStateException("Could not retrieve the version.");
        }
        return new Tuple<>(filtered_addresses, stackVersion);
    }

    protected String getCaFingerprint() throws Exception {
        final KeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
        if (keyConfig instanceof StoreKeyConfig == false) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration is " +
                "not configured with a keystore");
        }
        final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates =
            ((StoreKeyConfig) keyConfig).getPrivateKeyEntries(environment).stream()
                .filter(t -> t.v2().getBasicConstraints() != -1).collect(Collectors.toList());
        if (httpCaKeysAndCertificates.isEmpty()) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration " +
                "Keystore doesn't contain any PrivateKey entries where the associated certificate is a CA certificate");
        } else if (httpCaKeysAndCertificates.size() > 1) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration " +
                "Keystore contains multiple PrivateKey entries where the associated certificate is a CA certificate");
        }
        return SslUtil.calculateFingerprint(httpCaKeysAndCertificates.get(0).v2(), "SHA-256");
    }

    static List<String> getFilteredAddresses(List<String> addresses) throws Exception {
        List<String> filtered_addresses = new ArrayList<>();
        for (String bound_address : addresses){
            URI uri = new URI("http://" + bound_address);
            InetAddress inet_address = InetAddress.getByName(uri.getHost());
            if (inet_address.isLoopbackAddress() != true) {
                filtered_addresses.add(bound_address);
            }
        }
        return filtered_addresses.isEmpty() ? addresses : filtered_addresses;
    }
}
