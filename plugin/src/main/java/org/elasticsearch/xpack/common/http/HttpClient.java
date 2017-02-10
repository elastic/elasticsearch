/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.http;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.common.http.auth.ApplicableHttpAuth;
import org.elasticsearch.xpack.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.common.socket.SocketAccess;
import org.elasticsearch.xpack.ssl.SSLService;

import javax.net.ssl.HostnameVerifier;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpClient extends AbstractComponent {

    private static final String SETTINGS_SSL_PREFIX = "xpack.http.ssl.";

    private final HttpAuthRegistry httpAuthRegistry;
    private final CloseableHttpClient client;
    private final Integer proxyPort;
    private final String proxyHost;
    private final TimeValue defaultConnectionTimeout;
    private final TimeValue defaultReadTimeout;

    public HttpClient(Settings settings, HttpAuthRegistry httpAuthRegistry, SSLService sslService) {
        super(settings);
        this.httpAuthRegistry = httpAuthRegistry;
        this.defaultConnectionTimeout = HttpSettings.CONNECTION_TIMEOUT.get(settings);
        this.defaultReadTimeout = HttpSettings.READ_TIMEOUT.get(settings);

        // proxy setup
        this.proxyHost = HttpSettings.PROXY_HOST.get(settings);
        this.proxyPort = HttpSettings.PROXY_PORT.get(settings);
        if (proxyPort != 0 && Strings.hasText(proxyHost)) {
            logger.info("Using default proxy for http input and slack/hipchat/pagerduty/webhook actions [{}:{}]", proxyHost, proxyPort);
        } else if (proxyPort != 0 ^ Strings.hasText(proxyHost)) {
            throw new IllegalArgumentException("HTTP proxy requires both settings: [" + HttpSettings.PROXY_HOST.getKey() + "] and [" +
                    HttpSettings.PROXY_PORT.getKey() + "]");
        }

        HttpClientBuilder clientBuilder = HttpClientBuilder.create();

        // ssl setup
        Settings sslSettings = settings.getByPrefix(SETTINGS_SSL_PREFIX);
        boolean isHostnameVerificationEnabled = sslService.getVerificationMode(sslSettings, Settings.EMPTY).isHostnameVerificationEnabled();
        HostnameVerifier verifier = isHostnameVerificationEnabled ? new DefaultHostnameVerifier() : NoopHostnameVerifier.INSTANCE;
        SSLConnectionSocketFactory factory = new SSLConnectionSocketFactory(sslService.sslSocketFactory(sslSettings), verifier);
        clientBuilder.setSSLSocketFactory(factory);

        client = clientBuilder.build();
    }

    public HttpResponse execute(HttpRequest request) throws IOException {
        URI uri = createURI(request);

        HttpRequestBase internalRequest;
        if (request.method == HttpMethod.HEAD) {
            internalRequest = new HttpHead(uri);
        } else {
            HttpMethodWithEntity methodWithEntity = new HttpMethodWithEntity(uri, request.method.name());
            if (request.body != null) {
                methodWithEntity.setEntity(new StringEntity(request.body));
            }
            internalRequest = methodWithEntity;
        }
        internalRequest.setHeader(HttpHeaders.ACCEPT_CHARSET, StandardCharsets.UTF_8.name());

        RequestConfig.Builder config = RequestConfig.custom();

        // headers
        if (request.headers().isEmpty() == false) {
            for (Map.Entry<String, String> entry : request.headers.entrySet()) {
                internalRequest.setHeader(entry.getKey(), entry.getValue());
            }
        }

        // BWC - hack for input requests made to elasticsearch that do not provide the right content-type header!
        if (request.hasBody() && internalRequest.containsHeader("Content-Type") == false) {
            XContentType xContentType = XContentFactory.xContentType(request.body());
            if (xContentType != null) {
                internalRequest.setHeader("Content-Type", xContentType.mediaType());
            }
        }

        // proxy
        if (request.proxy != null && request.proxy.equals(HttpProxy.NO_PROXY) == false) {
            HttpHost proxy = new HttpHost(request.proxy.getHost(), request.proxy.getPort(), request.scheme.scheme());
            config.setProxy(proxy);
        } else if (proxyPort != null && Strings.hasText(proxyHost)) {
            HttpHost proxy = new HttpHost(proxyHost, proxyPort, request.scheme.scheme());
            config.setProxy(proxy);
        }

        HttpClientContext localContext = HttpClientContext.create();
        // auth
        if (request.auth() != null) {
            ApplicableHttpAuth applicableAuth = httpAuthRegistry.createApplicable(request.auth);
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            applicableAuth.apply(credentialsProvider, new AuthScope(request.host, request.port));
            localContext.setCredentialsProvider(credentialsProvider);

            // preemptive auth, no need to wait for a 401 first
            AuthCache authCache = new BasicAuthCache();
            BasicScheme basicAuth = new BasicScheme();
            authCache.put(new HttpHost(request.host, request.port, request.scheme.scheme()), basicAuth);
            localContext.setAuthCache(authCache);
        }

        // timeouts
        if (request.connectionTimeout() != null) {

            config.setConnectTimeout(Math.toIntExact(request.connectionTimeout.millis()));
        } else {
            config.setConnectTimeout(Math.toIntExact(defaultConnectionTimeout.millis()));
        }

        if (request.readTimeout() != null) {
            config.setSocketTimeout(Math.toIntExact(request.readTimeout.millis()));
            config.setConnectionRequestTimeout(Math.toIntExact(request.readTimeout.millis()));
        } else {
            config.setSocketTimeout(Math.toIntExact(defaultReadTimeout.millis()));
            config.setConnectionRequestTimeout(Math.toIntExact(defaultReadTimeout.millis()));
        }

        internalRequest.setConfig(config.build());

        try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(internalRequest, localContext))) {
            // headers
            Header[] headers = response.getAllHeaders();
            Map<String, String[]> responseHeaders = new HashMap<>(headers.length);
            for (Header header : headers) {
                if (responseHeaders.containsKey(header.getName())) {
                    String[] old = responseHeaders.get(header.getName());
                    String[] values = new String[old.length + 1];

                    System.arraycopy(old, 0, values, 0, old.length);
                    values[values.length - 1] = header.getValue();

                    responseHeaders.put(header.getName(), values);
                } else {
                    responseHeaders.put(header.getName(), new String[]{header.getValue()});
                }
            }

            final byte[] body;
            // not every response has a content, i.e. 204
            if (response.getEntity() == null) {
                body = new byte[0];
            } else {
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    try (InputStream is = response.getEntity().getContent()) {
                        Streams.copy(is, outputStream);
                    }
                    body = outputStream.toByteArray();
                }
            }
            return new HttpResponse(response.getStatusLine().getStatusCode(), body, responseHeaders);
        }
    }

    private URI createURI(HttpRequest request) {
        // this could be really simple, as the apache http client has a UriBuilder class, however this class is always doing
        // url path escaping, and we have done this already, so this would result in double escaping
        try {
            List<NameValuePair> qparams = new ArrayList<>(request.params.size());
            request.params.forEach((k, v) -> qparams.add(new BasicNameValuePair(k, v)));
            URI uri = URIUtils.createURI(request.scheme.scheme(), request.host, request.port, request.path,
                    URLEncodedUtils.format(qparams, "UTF-8"), null);

            return uri;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Helper class to have all HTTP methods except HEAD allow for an body, including GET
     */
    final class HttpMethodWithEntity extends HttpEntityEnclosingRequestBase {

        private final String methodName;

        HttpMethodWithEntity(final URI uri, String methodName) {
            this.methodName = methodName;
            setURI(uri);
        }

        @Override
        public String getMethod() {
            return methodName;
        }
    }
}
