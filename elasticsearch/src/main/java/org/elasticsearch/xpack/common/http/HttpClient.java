/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.http;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.common.http.auth.ApplicableHttpAuth;
import org.elasticsearch.xpack.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.ssl.SSLService;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Client class to wrap http connections
 */
public class HttpClient extends AbstractComponent {

    static final String SETTINGS_SSL_PREFIX = "xpack.http.ssl.";
    static final String SETTINGS_PROXY_PREFIX = "xpack.http.proxy.";

    static final String SETTINGS_PROXY_HOST = SETTINGS_PROXY_PREFIX + "host";
    static final String SETTINGS_PROXY_PORT = SETTINGS_PROXY_PREFIX + "port";

    private final HttpAuthRegistry httpAuthRegistry;
    private final TimeValue defaultConnectionTimeout;
    private final TimeValue defaultReadTimeout;
    private final boolean isHostnameVerificationEnabled;
    private final SSLSocketFactory sslSocketFactory;
    private final HttpProxy proxy;

    public HttpClient(Settings settings, HttpAuthRegistry httpAuthRegistry, SSLService sslService) {
        super(settings);
        this.httpAuthRegistry = httpAuthRegistry;
        this.defaultConnectionTimeout = settings.getAsTime("xpack.http.default_connection_timeout", TimeValue.timeValueSeconds(10));
        this.defaultReadTimeout = settings.getAsTime("xpack.http.default_read_timeout", TimeValue.timeValueSeconds(10));
        Integer proxyPort = settings.getAsInt(SETTINGS_PROXY_PORT, null);
        String proxyHost = settings.get(SETTINGS_PROXY_HOST, null);
        if (proxyPort != null && Strings.hasText(proxyHost)) {
            this.proxy = new HttpProxy(proxyHost, proxyPort);
            logger.info("Using default proxy for http input and slack/hipchat/pagerduty/webhook actions [{}:{}]", proxyHost, proxyPort);
        } else if (proxyPort == null && Strings.hasText(proxyHost) == false) {
            this.proxy = HttpProxy.NO_PROXY;
        } else {
            throw new IllegalArgumentException("HTTP Proxy requires both settings: [" + SETTINGS_PROXY_HOST + "] and [" +
                    SETTINGS_PROXY_PORT + "]");
        }
        Settings sslSettings = settings.getByPrefix(SETTINGS_SSL_PREFIX);
        this.sslSocketFactory = sslService.sslSocketFactory(settings.getByPrefix(SETTINGS_SSL_PREFIX));
        this.isHostnameVerificationEnabled = sslService.getVerificationMode(sslSettings, Settings.EMPTY).isHostnameVerificationEnabled();
    }

    public HttpResponse execute(HttpRequest request) throws IOException {
        try {
            return doExecute(request);
        } catch (SocketTimeoutException ste) {
            throw new ElasticsearchTimeoutException("failed to execute http request. timeout expired", ste);
        }
    }

    public HttpResponse doExecute(HttpRequest request) throws IOException {
        String queryString = null;
        if (request.params() != null && !request.params().isEmpty()) {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, String> entry : request.params().entrySet()) {
                if (builder.length() != 0) {
                    builder.append('&');
                }
                builder.append(URLEncoder.encode(entry.getKey(), "UTF-8"))
                        .append('=')
                        .append(URLEncoder.encode(entry.getValue(), "UTF-8"));
            }
            queryString = builder.toString();
        }

        String path = Strings.hasLength(request.path) ? request.path : "";
        if (Strings.hasLength(queryString)) {
            path += "?" + queryString;
        }
        URL url = new URL(request.scheme.scheme(), request.host, request.port, path);

        logger.debug("making [{}] request to [{}]", request.method().method(), url);
        logger.trace("sending [{}] as body of request", request.body());

        // proxy configured in the request always wins!
        HttpProxy proxyToUse = request.proxy != null ? request.proxy : proxy;

        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection(proxyToUse.proxy());
        if (urlConnection instanceof HttpsURLConnection) {
            final HttpsURLConnection httpsConn = (HttpsURLConnection) urlConnection;
            final SSLSocketFactory factory = sslSocketFactory;
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    httpsConn.setSSLSocketFactory(factory);
                    if (isHostnameVerificationEnabled == false) {
                        httpsConn.setHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                    }
                    return null;
                }
            });
        }

        urlConnection.setRequestMethod(request.method().method());
        if (request.headers() != null) {
            for (Map.Entry<String, String> entry : request.headers().entrySet()) {
                urlConnection.setRequestProperty(entry.getKey(), entry.getValue());
            }
        }
        if (request.auth() != null) {
            logger.trace("applying auth headers");
            ApplicableHttpAuth applicableAuth = httpAuthRegistry.createApplicable(request.auth);
            applicableAuth.apply(urlConnection);
        }
        urlConnection.setUseCaches(false);
        urlConnection.setRequestProperty("Accept-Charset", StandardCharsets.UTF_8.name());
        if (request.body() != null) {
            urlConnection.setDoOutput(true);
            byte[] bytes = request.body().getBytes(StandardCharsets.UTF_8.name());
            urlConnection.setRequestProperty("Content-Length", String.valueOf(bytes.length));
            urlConnection.getOutputStream().write(bytes);
            urlConnection.getOutputStream().close();
        }

        TimeValue connectionTimeout = request.connectionTimeout != null ? request.connectionTimeout : defaultConnectionTimeout;
        urlConnection.setConnectTimeout((int) connectionTimeout.millis());

        TimeValue readTimeout = request.readTimeout != null ? request.readTimeout : defaultReadTimeout;
        urlConnection.setReadTimeout((int) readTimeout.millis());

        urlConnection.connect();

        final int statusCode = urlConnection.getResponseCode();
        // no status code, not considered a valid HTTP response then
        if (statusCode == -1) {
            throw new IOException("Not a valid HTTP response, no status code in response");
        }
        Map<String, String[]> responseHeaders = new HashMap<>(urlConnection.getHeaderFields().size());
        for (Map.Entry<String, List<String>> header : urlConnection.getHeaderFields().entrySet()) {
            // HttpURLConnection#getHeaderFields returns the first status line as a header
            // with a `null` key (facepalm)... so we have to skip that one.
            if (header.getKey() != null) {
                responseHeaders.put(header.getKey(), header.getValue().toArray(new String[header.getValue().size()]));
            }
        }
        logger.debug("http status code [{}]", statusCode);
        final byte[] body;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            try (InputStream is = urlConnection.getInputStream()) {
                Streams.copy(is, outputStream);
            } catch (Exception e) {
                if (urlConnection.getErrorStream() != null) {
                    try (InputStream is = urlConnection.getErrorStream()) {
                        Streams.copy(is, outputStream);
                    }
                }
            }
            body = outputStream.toByteArray();
        }
        return new HttpResponse(statusCode, body, responseHeaders);
    }

    private static final class NoopHostnameVerifier implements HostnameVerifier {

        private static final HostnameVerifier INSTANCE = new NoopHostnameVerifier();

        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    }
}
