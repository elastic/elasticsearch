/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.HttpResponse.HttpResponseBuilder;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.MessageDigest;
import java.security.PrivilegedExceptionAction;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PORT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT;

/**
 * A simple http client for usage in command line tools. This client only uses internal jdk classes and does
 * not rely on an external http libraries.
 */
public class CommandLineHttpClient {

    /**
     * Timeout HTTP(s) reads after 35 seconds.
     * The default timeout for discovering a master is 30s, and we want to be longer than this, otherwise a querying a disconnected node
     * will trigger as client side timeout rather than giving clear error details.
     */
    private static final int READ_TIMEOUT = 35 * 1000;

    private final Environment env;
    private final String pinnedCaCertFingerprint;

    public CommandLineHttpClient(Environment env) {
        this.env = env;
        this.pinnedCaCertFingerprint = null;
    }

    public CommandLineHttpClient(Environment env, String pinnedCaCertFingerprint) {
        this.env = env;
        this.pinnedCaCertFingerprint = pinnedCaCertFingerprint;
    }

    /**
     * General purpose HTTP(S) call with JSON Content-Type and Authorization Header.
     * SSL settings are read from the settings file, if any.
     *
     * @param user
     *            user in the authorization header.
     * @param password
     *            password in the authorization header.
     * @param requestBodySupplier
     *            supplier for the JSON string body of the request.
     * @param responseHandler
     *            handler of the response Input Stream.
     * @return HTTP protocol response code.
     */
    public HttpResponse execute(
        String method,
        URL url,
        String user,
        SecureString password,
        CheckedSupplier<String, Exception> requestBodySupplier,
        CheckedFunction<InputStream, HttpResponseBuilder, Exception> responseHandler
    ) throws Exception {

        final String authorizationHeader = UsernamePasswordToken.basicAuthHeaderValue(user, password);
        return execute(method, url, authorizationHeader, requestBodySupplier, responseHandler);
    }

    /**
     * General purpose HTTP(S) call with JSON Content-Type and Authorization Header.
     * SSL settings are read from the settings file, if any.
     *
     * @param apiKey
     *            API key value to be used in the Authorization header
     * @param requestBodySupplier
     *            supplier for the JSON string body of the request.
     * @param responseHandler
     *            handler of the response Input Stream.
     * @return HTTP protocol response code.
     */
    public HttpResponse execute(
        String method,
        URL url,
        SecureString apiKey,
        CheckedSupplier<String, Exception> requestBodySupplier,
        CheckedFunction<InputStream, HttpResponseBuilder, Exception> responseHandler
    ) throws Exception {
        final String authorizationHeaderValue = apiKeyHeaderValue(apiKey);
        return execute(method, url, authorizationHeaderValue, requestBodySupplier, responseHandler);
    }

    @SuppressForbidden(reason = "We call connect in doPrivileged and provide SocketPermission")
    private HttpResponse execute(
        String method,
        URL url,
        String authorizationHeader,
        CheckedSupplier<String, Exception> requestBodySupplier,
        CheckedFunction<InputStream, HttpResponseBuilder, Exception> responseHandler
    ) throws Exception {
        final HttpURLConnection conn;
        // If using SSL, need a custom service because it's likely a self-signed certificate
        if ("https".equalsIgnoreCase(url.getProtocol())) {
            final SSLService sslService = new SSLService(env);
            final HttpsURLConnection httpsConn = (HttpsURLConnection) url.openConnection();
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                if (pinnedCaCertFingerprint != null) {
                    final SSLContext sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(null, new TrustManager[] { fingerprintTrustingTrustManager(pinnedCaCertFingerprint) }, null);
                    httpsConn.setSSLSocketFactory(sslContext.getSocketFactory());
                } else {
                    final SslConfiguration sslConfiguration = sslService.getHttpTransportSSLConfiguration();
                    // Requires permission java.lang.RuntimePermission "setFactory";
                    httpsConn.setSSLSocketFactory(sslService.sslSocketFactory(sslConfiguration));
                    final boolean isHostnameVerificationEnabled = sslConfiguration.verificationMode().isHostnameVerificationEnabled();
                    if (isHostnameVerificationEnabled == false) {
                        httpsConn.setHostnameVerifier((hostname, session) -> true);
                    }
                }
                return null;
            });
            conn = httpsConn;
        } else {
            conn = (HttpURLConnection) url.openConnection();
        }
        conn.setRequestMethod(method);
        conn.setReadTimeout(READ_TIMEOUT);
        // Add basic-auth header
        conn.setRequestProperty("Authorization", authorizationHeader);
        conn.setRequestProperty("Content-Type", XContentType.JSON.mediaType());
        String bodyString = requestBodySupplier.get();
        conn.setDoOutput(bodyString != null); // set true if we are sending a body
        SocketAccess.doPrivileged(conn::connect);
        if (bodyString != null) {
            try (OutputStream out = conn.getOutputStream()) {
                out.write(bodyString.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                Releasables.closeWhileHandlingException(conn::disconnect);
                throw e;
            }
        }
        // this throws IOException if there is a network problem
        final int responseCode = conn.getResponseCode();
        HttpResponseBuilder responseBuilder;
        try (InputStream inputStream = conn.getInputStream()) {
            responseBuilder = responseHandler.apply(inputStream);
        } catch (IOException e) {
            // this IOException is if the HTTP response code is 'BAD' (>= 400)
            try (InputStream errorStream = conn.getErrorStream()) {
                responseBuilder = responseHandler.apply(errorStream);
            }
        } finally {
            Releasables.closeWhileHandlingException(conn::disconnect);
        }
        responseBuilder.withHttpStatus(responseCode);
        return responseBuilder.build();
    }

    public String getDefaultURL() {
        final Settings settings = env.settings();
        final String scheme = XPackSettings.HTTP_SSL_ENABLED.get(settings) ? "https" : "http";
        List<String> httpPublishHost = SETTING_HTTP_PUBLISH_HOST.get(settings);
        if (httpPublishHost.isEmpty()) {
            httpPublishHost = NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings);
        }

        // we cannot do custom name resolution here...
        NetworkService networkService = new NetworkService(Collections.emptyList());
        try {
            InetAddress publishAddress = networkService.resolvePublishHostAddresses(httpPublishHost.toArray(Strings.EMPTY_ARRAY));
            int port = SETTING_HTTP_PUBLISH_PORT.get(settings);
            if (port <= 0) {
                int[] ports = SETTING_HTTP_PORT.get(settings).ports();
                if (ports.length > 0) {
                    port = ports[0];
                }

                // this sucks but a port can be specified with a value of 0, we'll never be able to connect to it so just default to
                // what we know
                if (port <= 0) {
                    throw new IllegalStateException("unable to determine http port from settings");
                }
            }
            return scheme + "://" + InetAddresses.toUriString(publishAddress) + ":" + port;
        } catch (Exception e) {
            throw new IllegalStateException(
                "unable to determine default URL from settings, please use the -u option to explicitly " + "provide the url",
                e
            );
        }
    }

    public static String getErrorCause(HttpResponse httpResponse) {
        final Object error = httpResponse.getResponseBody().get("error");
        if (error == null) {
            return null;
        }
        if (error instanceof Map) {
            Object reason = ((Map) error).get("reason");
            if (reason != null) {
                return reason.toString();
            }
            final Object root = ((Map) error).get("root_cause");
            if (root != null && root instanceof Map) {
                reason = ((Map) root).get("reason");
                if (reason != null) {
                    return reason.toString();
                }
                final Object type = ((Map) root).get("type");
                if (type != null) {
                    return (String) type;
                }
            }
            return String.valueOf(((Map) error).get("type"));
        }
        return error.toString();
    }

    /**
     * If cluster is not up yet (connection refused or master is unavailable), we will retry @retries number of times
     * If status is 'Red', we will wait for 'Yellow' for 30s (default timeout)
     */
    public void checkClusterHealthWithRetriesWaitingForCluster(String username, SecureString password, int retries) throws Exception {
        final URL clusterHealthUrl = createURL(new URL(getDefaultURL()), "_cluster/health", "?wait_for_status=yellow&pretty");
        HttpResponse response;
        try {
            response = execute("GET", clusterHealthUrl, username, password, () -> null, CommandLineHttpClient::responseBuilder);
        } catch (Exception e) {
            if (retries > 0) {
                Thread.sleep(1000);
                retries -= 1;
                checkClusterHealthWithRetriesWaitingForCluster(username, password, retries);
                return;
            } else {
                throw new IllegalStateException("Failed to determine the health of the cluster. ", e);
            }
        }
        final int responseStatus = response.getHttpStatus();
        if (responseStatus != HttpURLConnection.HTTP_OK) {
            if (responseStatus != HttpURLConnection.HTTP_UNAVAILABLE) {
                if (retries > 0) {
                    Thread.sleep(1000);
                    retries -= 1;
                    checkClusterHealthWithRetriesWaitingForCluster(username, password, retries);
                    return;
                } else {
                    throw new IllegalStateException(
                        "Failed to determine the health of the cluster. Unexpected http status [" + responseStatus + "]"
                    );
                }
            }
            throw new IllegalStateException(
                "Failed to determine the health of the cluster. Unexpected http status [" + responseStatus + "]"
            );
        } else {
            final String clusterStatus = Objects.toString(response.getResponseBody().get("status"), "");
            if (clusterStatus.isEmpty()) {
                throw new IllegalStateException(
                    "Failed to determine the health of the cluster. Cluster health API did not return a status value."
                );
            } else if ("red".equalsIgnoreCase(clusterStatus)) {
                throw new IllegalStateException("Failed to determine the health of the cluster. Cluster health is currently RED.");
            }
            // else it is yellow or green so we can continue
        }
    }

    public static HttpResponse.HttpResponseBuilder responseBuilder(InputStream is) throws IOException {
        final HttpResponse.HttpResponseBuilder httpResponseBuilder = new HttpResponse.HttpResponseBuilder();
        final String responseBody = Streams.readFully(is).utf8ToString();
        httpResponseBuilder.withResponseBody(responseBody);
        return httpResponseBuilder;
    }

    public static URL createURL(URL url, String path, String query) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + path).replaceAll("/+", "/") + query);
    }

    public static String apiKeyHeaderValue(SecureString apiKey) {
        CharBuffer chars = CharBuffer.allocate(apiKey.length());
        byte[] charBytes = null;
        try {
            chars.put(apiKey.getChars());
            charBytes = CharArrays.toUtf8Bytes(chars.array());

            // TODO we still have passwords in Strings in headers. Maybe we can look into using a CharSequence?
            String apiKeyToken = Base64.getEncoder().encodeToString(charBytes);
            return "ApiKey " + apiKeyToken;
        } finally {
            Arrays.fill(chars.array(), (char) 0);
            if (charBytes != null) {
                Arrays.fill(charBytes, (byte) 0);
            }
        }
    }

    /**
     * Returns a TrustManager to be used in a client SSLContext, which trusts all certificates that are signed
     * by a specific CA certificate ( identified by its SHA256 fingerprint, {@code pinnedCaCertFingerPrint} )
     */
    private static TrustManager fingerprintTrustingTrustManager(String caCertFingerprint) {
        final TrustManager trustManager = new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {}

            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                if (chain.length < 2) {
                    throw new CertificateException("CA certificate not in chain, or self-signed certificate");
                }
                final Certificate caCertFromChain = chain[1];
                MessageDigest sha256 = MessageDigests.sha256();
                sha256.update(caCertFromChain.getEncoded());
                if (MessageDigests.toHexString(sha256.digest()).equals(caCertFingerprint) == false) {
                    throw new CertificateException();
                }
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };

        return trustManager;
    }
}
