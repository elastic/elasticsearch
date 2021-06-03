/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.ssl.KeyConfig;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.StoreKeyConfig;
import org.elasticsearch.xpack.security.authc.esnative.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.authc.esnative.tool.HttpResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CreateEnrollmentToken {
    protected static final String ENROLL_API_KEY_EXPIRATION_SEC = "30m";

    private static final Logger logger = LogManager.getLogger(CreateEnrollmentToken.class);
    private final Environment environment;
    private final SSLService sslService;
    private final CommandLineHttpClient client;

    public CreateEnrollmentToken(Environment environment) {
        this(environment, new CommandLineHttpClient(environment));
    }

    // protected for testing
    protected CreateEnrollmentToken(Environment environment, CommandLineHttpClient client) {

        this.environment = environment;
        this.sslService = new SSLService(environment);
        this.client = client;
    }

    public String create(Terminal terminal, String user, SecureString password) throws Exception {
        final KeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
        if (keyConfig instanceof StoreKeyConfig == false) {
            throw new UserException(ExitCodes.CONFIG,
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration is not configured with a keystore");
        }
        final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates =
            ((StoreKeyConfig) keyConfig).getPrivateKeyEntries(environment).stream()
                .filter(t -> t.v2().getBasicConstraints() != -1).collect(Collectors.toList());
        if (httpCaKeysAndCertificates.isEmpty()) {
            throw new UserException(ExitCodes.CONFIG,
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration Keystore doesn't contain any " +
                    "PrivateKey entries where the associated certificate is a CA certificate");
        } else if (httpCaKeysAndCertificates.size() > 1) {
            throw new UserException(ExitCodes.CONFIG,
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration Keystore contain multiple " +
                    "PrivateKey entries where the associated certificate is a CA certificate");
        } else {
            final CheckedSupplier<String, Exception> createApiKeyRequestBodySupplier = () -> {
                XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                xContentBuilder.startObject()
                    .field("name", "enrollment_token_API_key_" + UUIDs.base64UUID())
                    .field("expiration", ENROLL_API_KEY_EXPIRATION_SEC)
                    .startObject("role_descriptors")
                    .startObject("create_enrollment_token")
                    .field("cluster", "[" + ClusterPrivilegeResolver.ENROLL_NODE.name() + "]")
                    .endObject()
                    .endObject()
                    .endObject();
                return Strings.toString(xContentBuilder);
            };

            final CheckedSupplier<String, Exception> getHttpInfoRequestBodySupplier = () -> {
                XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                xContentBuilder.startObject()
                    .endObject();
                return Strings.toString(xContentBuilder);
            };

            try {
                final URL url = new URL(client.getDefaultURL());
                final HttpResponse httpResponseApiKey = client.execute("POST", createAPIKeyURL(url), user, password,
                    createApiKeyRequestBodySupplier, is -> responseBuilder(is, terminal));
                int httpCode = httpResponseApiKey.getHttpStatus();

                if (httpCode != HttpURLConnection.HTTP_OK) {
                    throw new UserException(ExitCodes.CANT_CREATE,
                        "Unexpected response code [" + httpCode + "] from calling POST " + createAPIKeyURL(url));
                }

                final String apiKey = Objects.toString(httpResponseApiKey.getResponseBody().get("api_key"), "");
                if (apiKey == null || apiKey.isEmpty()) {
                    throw new UserException(ExitCodes.CANT_CREATE,
                        "Could not create an api key.");
                }

                final HttpResponse httpResponseHttp = client.execute("GET", getHttpInfoURL(url), user, password,
                    getHttpInfoRequestBodySupplier, is -> responseBuilder(is, terminal));
                httpCode = httpResponseHttp.getHttpStatus();

                if (httpCode != HttpURLConnection.HTTP_OK) {
                    throw new UserException(ExitCodes.CANT_CREATE,
                        "Unexpected response code [" + httpCode + "] from calling GET " + getHttpInfoURL(url));
                }

                final String address = getPublishedAddress(httpResponseHttp.getResponseBody());
                if (address == null || address.isEmpty()) {
                    throw new UserException(ExitCodes.CANT_CREATE,
                        "Could not retrieve the ip address.");
                }

                final String stackVersion = getVersion(httpResponseHttp.getResponseBody());
                if (stackVersion == null || stackVersion.isEmpty()) {
                    throw new UserException(ExitCodes.CANT_CREATE,
                        "Could not retrieve the version.");
                }

                final String fingerprint = SslUtil.calculateFingerprint(httpCaKeysAndCertificates.get(0).v2());

                final XContentBuilder builder = JsonXContent.contentBuilder();
                builder.startObject();
                builder.startArray("adr");
                builder.value(address);
                builder.endArray();
                builder.field("fgr", fingerprint);
                builder.field("key", apiKey);//  CreateApiKeyResponse.getKey().toString());
                builder.endObject();
                final String jsonString = Strings.toString(builder);
                final String token = Base64.getUrlEncoder().encodeToString(jsonString.getBytes(StandardCharsets.UTF_8));
                return stackVersion + "." + token;
            } catch (Exception e) {
                logger.error(("Error generating enrollment token"), e);
                throw new UserException(ExitCodes.CANT_CREATE, "Error generating enrollment token: " + e.getMessage());
            }
        }
    }

    private HttpResponse.HttpResponseBuilder responseBuilder(InputStream is, Terminal terminal) throws IOException {
        final HttpResponse.HttpResponseBuilder httpResponseBuilder = new HttpResponse.HttpResponseBuilder();
        if (is != null) {
            byte[] bytes = toByteArray(is);
            String responseBody = new String(bytes, StandardCharsets.UTF_8);
            terminal.println(Terminal.Verbosity.VERBOSE, responseBody);
            httpResponseBuilder.withResponseBody(responseBody);
        } else {
            terminal.println(Terminal.Verbosity.VERBOSE, "<Empty response>");
        }
        return httpResponseBuilder;
    }

    private byte[] toByteArray(InputStream is) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] internalBuffer = new byte[1024];
        int read = is.read(internalBuffer);
        while (read != -1) {
            baos.write(internalBuffer, 0, read);
            read = is.read(internalBuffer);
        }
        return baos.toByteArray();
    }

    protected static URL createAPIKeyURL(URL url) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_security/api_key").replaceAll("/+", "/"));
    }

    protected static URL getHttpInfoURL(URL url) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + "/_nodes/http").replaceAll("/+", "/"));
    }

    protected static String getPublishedAddress(Map<?, ?> nodesInfo) {
        nodesInfo = (Map<?, ?>) nodesInfo.get("nodes");
        Map<?, ?> nodeInfo = (Map<?, ?>) nodesInfo.values().iterator().next();
        Map<?, ?> http = (Map<?, ?>) nodeInfo.get("http");
        return http.get("publish_address").toString();
    }

    protected static String getVersion(Map<?, ?> nodesInfo) {
        nodesInfo = (Map<?, ?>) nodesInfo.get("nodes");
        Map<?, ?> nodeInfo = (Map<?, ?>) nodesInfo.values().iterator().next();
        return nodeInfo.get("version").toString();
    }
}
