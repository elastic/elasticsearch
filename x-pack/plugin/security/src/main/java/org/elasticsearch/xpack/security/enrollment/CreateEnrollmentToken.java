/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.ssl.SslConfigException;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CreateEnrollmentToken {
    protected static final String ENROLL_API_KEY_EXPIRATION = "30m";

    private static final Logger logger = LogManager.getLogger(CreateEnrollmentToken.class);
    private final Environment environment;
    private final CommandLineHttpClient client;

    public CreateEnrollmentToken(Environment environment) {
        this(environment, new CommandLineHttpClient(environment));
    }

    // protected for testing
    protected CreateEnrollmentToken(Environment environment, CommandLineHttpClient client) {
        this.environment = environment;
        this.client = client;
    }

    public String create(String user, SecureString password, SecureString keystore_password) throws Exception {
        if (XPackSettings.ENROLLMENT_ENABLED.get(environment.settings()) != true) {
            throw new IllegalStateException("'xpack.security.enrollment' must be enabled to create an enrollment token");
        }
        String fingerprint = getFingerprint(keystore_password);
        String apiKey = getApiKey(user, password);
        Tuple<List<String>, String> httpInfo = getAddressesAndVersion(user, password);

        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.field("ver", httpInfo.v2());
            builder.startArray("adr");
            for (String bound_address : httpInfo.v1()) {
                builder.value(bound_address);
            }
            builder.endArray();
            builder.field("fgr", fingerprint);
            builder.field("key", apiKey);
            builder.endObject();
            final String jsonString = Strings.toString(builder);
            return Base64.getUrlEncoder().encodeToString(jsonString.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error(("Error generating enrollment token"), e);
            throw new IllegalStateException("Error generating enrollment token: " + e.getMessage());
        }
    }

    private HttpResponse.HttpResponseBuilder responseBuilder(InputStream is) throws IOException {
        final HttpResponse.HttpResponseBuilder httpResponseBuilder = new HttpResponse.HttpResponseBuilder();
        if (is != null) {
            byte[] bytes = toByteArray(is);
            String responseBody = new String(bytes, StandardCharsets.UTF_8);
            logger.debug(responseBody);
            httpResponseBuilder.withResponseBody(responseBody);
        } else {
            logger.debug("<Empty response>");
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

    protected String getApiKey(String user, SecureString password) throws Exception {
        final CheckedSupplier<String, Exception> createApiKeyRequestBodySupplier = () -> {
            XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
            xContentBuilder.startObject()
                .field("name", "enrollment_token_API_key_" + UUIDs.base64UUID())
                .field("expiration", ENROLL_API_KEY_EXPIRATION)
                .startObject("role_descriptors")
                .startObject("create_enrollment_token")
                .field("cluster", "[cluster:admin/xpack/security/enrollment/enroll/node]")
                .endObject()
                .endObject()
                .endObject();
            return Strings.toString(xContentBuilder);
        };

        final URL url = new URL(client.getDefaultURL());
        final URL Url = createAPIKeyURL(url);
        final HttpResponse httpResponseApiKey = client.execute("POST", Url, user, password,
            createApiKeyRequestBodySupplier, is -> responseBuilder(is));
        final int httpCode = httpResponseApiKey.getHttpStatus();

        if (httpCode != HttpURLConnection.HTTP_OK) {
            throw new IllegalStateException("Unexpected response code [" + httpCode + "] from calling POST "
                + Url);
        }

        final String apiKey = Objects.toString(httpResponseApiKey.getResponseBody().get("api_key"), "");
        if (Strings.isNullOrEmpty(apiKey)) {
            throw new IllegalStateException("Could not create an api key.");
        }
        return apiKey;
    }

    protected Tuple<List<String>, String> getAddressesAndVersion(String user, SecureString password) throws Exception {
        final CheckedSupplier<String, Exception> getHttpInfoRequestBodySupplier = () -> {
            XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
            xContentBuilder.startObject()
                .endObject();
            return Strings.toString(xContentBuilder);
        };

        final URL url = new URL(client.getDefaultURL());
        final HttpResponse httpResponseHttp = client.execute("GET", getHttpInfoURL(url), user, password,
            getHttpInfoRequestBodySupplier, is -> responseBuilder(is));
        final int httpCode = httpResponseHttp.getHttpStatus();

        if (httpCode != HttpURLConnection.HTTP_OK) {
            throw new IllegalStateException("Unexpected response code [" + httpCode + "] from calling GET " + getHttpInfoURL(url));
        }

        final List<String> addresses = getBoundAddresses(httpResponseHttp.getResponseBody());
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalStateException("Could not retrieve the ip address.");
        }
        final List<String> filtered_addresses = getFilteredAddresses(addresses);

        final String stackVersion = getVersion(httpResponseHttp.getResponseBody());
        if (stackVersion == null || stackVersion.isEmpty()) {
            throw new IllegalStateException("Could not retrieve the version.");
        }
        return new Tuple<>(filtered_addresses, stackVersion);
    }

    protected String getFingerprint(SecureString keystore_password) throws Exception {
        String keyStorePath = environment.settings().get("xpack.security.http.ssl.keystore.path");
        if(Strings.isNullOrEmpty(keyStorePath)) {
            throw new IllegalStateException("'xpack.security.http.ssl.keystore.path' is not configured");
        }
        Path path = environment.configFile().resolve(keyStorePath);
        KeyStore keyStore = readKeyStore(path, keystore_password);
        final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates =
        getPrivateKeyEntries(keyStore, keystore_password).stream()
            .filter(t -> t.v2().getBasicConstraints() != -1).collect(Collectors.toList());
        if(httpCaKeysAndCertificates.isEmpty()) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration " +
                "Keystore doesn't contain any PrivateKey entries where the associated certificate is a CA certificate");
        } else if(httpCaKeysAndCertificates.size()>1) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration " +
                "Keystore contain multiple PrivateKey entries where the associated certificate is a CA certificate");
        }
        return SslUtil.calculateFingerprint(httpCaKeysAndCertificates.get(0).v2());
    }

    static List<String> getFilteredAddresses(List<String> addresses) throws Exception {
        List<String> filtered_addresses = new ArrayList<>();
        for (String bound_address : addresses){
            URI uri = new URI("http://" + bound_address);
            InetAddress inet_address = InetAddress.getByName(uri.getHost());
            if (inet_address.isLoopbackAddress() != true || inet_address.isSiteLocalAddress() == true) {
                filtered_addresses.add(bound_address);
            }
        }
        return filtered_addresses.isEmpty() ? addresses : filtered_addresses;
    }

    static KeyStore readKeyStore(Path path, SecureString password) throws GeneralSecurityException {
        final String type = inferKeyStoreType(path);
        if (Files.notExists(path)) {
            throw new SslConfigException("cannot read a [" + type + "] keystore from [" + path.toAbsolutePath()
                + "] because the file does not exist");
        }
        try {
            KeyStore keyStore = KeyStore.getInstance(type);
            try (InputStream in = Files.newInputStream(path)) {
                keyStore.load(in, password.getChars());
            }
            return keyStore;
        } catch (IOException e) {
            throw new SslConfigException("cannot read a [" + type + "] keystore from [" + path.toAbsolutePath() + "] - " + e.getMessage(),
                e);
        }
    }

    static String inferKeyStoreType(Path path) {
        String name = path == null ? "" : path.toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".p12") || name.endsWith(".pfx") || name.endsWith(".pkcs12")) {
            return "PKCS12";
        } else {
            return "jks";
        }
    }

    static List<Tuple<PrivateKey, X509Certificate>> getPrivateKeyEntries(KeyStore keyStore, SecureString password) {
        try {
            List<Tuple<PrivateKey, X509Certificate>> entries = new ArrayList<>();
            for (Enumeration<String> e = keyStore.aliases(); e.hasMoreElements(); ) {
                final String alias = e.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    Key key = keyStore.getKey(alias, password.getChars());
                    Certificate certificate = keyStore.getCertificate(alias);
                    if (key instanceof PrivateKey && certificate instanceof X509Certificate) {
                        entries.add(Tuple.tuple((PrivateKey) key, (X509Certificate) certificate));
                    }
                }
            }
            return entries;
        } catch (Exception e) {
            throw new ElasticsearchException("failed to list keys and certificates", e);
        }
    }
}
