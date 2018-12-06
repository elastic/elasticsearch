/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
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
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;

import javax.net.ssl.HostnameVerifier;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpClient implements Closeable {

    private static final String SETTINGS_SSL_PREFIX = "xpack.http.ssl.";
    // picking a reasonable high value here to allow for setups with lots of watch executions or many http inputs/actions
    // this is also used as the value per route, if you are connecting to the same endpoint a lot, which is likely, when
    // you are querying a remote Elasticsearch cluster
    private static final int MAX_CONNECTIONS = 500;
    private static final Logger logger = LogManager.getLogger(HttpClient.class);

    private final CloseableHttpClient client;
    private final HttpProxy settingsProxy;
    private final TimeValue defaultConnectionTimeout;
    private final TimeValue defaultReadTimeout;
    private final ByteSizeValue maxResponseSize;
    private final CryptoService cryptoService;

    public HttpClient(Settings settings, SSLService sslService, CryptoService cryptoService) {
        this.defaultConnectionTimeout = HttpSettings.CONNECTION_TIMEOUT.get(settings);
        this.defaultReadTimeout = HttpSettings.READ_TIMEOUT.get(settings);
        this.maxResponseSize = HttpSettings.MAX_HTTP_RESPONSE_SIZE.get(settings);
        this.settingsProxy = getProxyFromSettings(settings);
        this.cryptoService = cryptoService;

        HttpClientBuilder clientBuilder = HttpClientBuilder.create();

        // ssl setup
        SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(SETTINGS_SSL_PREFIX);
        boolean isHostnameVerificationEnabled = sslConfiguration.verificationMode().isHostnameVerificationEnabled();
        HostnameVerifier verifier = isHostnameVerificationEnabled ? new DefaultHostnameVerifier() : NoopHostnameVerifier.INSTANCE;
        SSLConnectionSocketFactory factory = new SSLConnectionSocketFactory(sslService.sslSocketFactory(sslConfiguration), verifier);
        clientBuilder.setSSLSocketFactory(factory);

        clientBuilder.evictExpiredConnections();
        clientBuilder.setMaxConnPerRoute(MAX_CONNECTIONS);
        clientBuilder.setMaxConnTotal(MAX_CONNECTIONS);

        client = clientBuilder.build();
    }

    public HttpResponse execute(HttpRequest request) throws IOException {
        URI uri = createURI(request);

        HttpRequestBase internalRequest;
        if (request.method == HttpMethod.HEAD) {
            internalRequest = new HttpHead(uri);
        } else {
            HttpMethodWithEntity methodWithEntity = new HttpMethodWithEntity(uri, request.method.name());
            if (request.hasBody()) {
                ByteArrayEntity entity = new ByteArrayEntity(request.body.getBytes(StandardCharsets.UTF_8));
                String contentType = request.headers().get(HttpHeaders.CONTENT_TYPE);
                if (Strings.hasLength(contentType)) {
                    entity.setContentType(contentType);
                } else {
                    entity.setContentType(ContentType.TEXT_PLAIN.toString());
                }
                methodWithEntity.setEntity(entity);
            }
            internalRequest = methodWithEntity;
        }
        internalRequest.setHeader(HttpHeaders.ACCEPT_CHARSET, StandardCharsets.UTF_8.name());

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

        RequestConfig.Builder config = RequestConfig.custom();
        setProxy(config, request, settingsProxy);
        HttpClientContext localContext = HttpClientContext.create();
        // auth
        if (request.auth() != null) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            Credentials credentials = new UsernamePasswordCredentials(request.auth().username,
                new String(request.auth().password.text(cryptoService)));
            credentialsProvider.setCredentials(new AuthScope(request.host, request.port), credentials);
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
                    try (InputStream is = new SizeLimitInputStream(maxResponseSize, response.getEntity().getContent())) {
                        Streams.copy(is, outputStream);
                    }
                    body = outputStream.toByteArray();
                }
            }
            return new HttpResponse(response.getStatusLine().getStatusCode(), body, responseHeaders);
        }
    }

    /**
     * Enriches the config object optionally with proxy information
     *
     * @param config    The request builder config object
     * @param request   The request parsed into the HTTP client
     */
    static void setProxy(RequestConfig.Builder config, HttpRequest request, HttpProxy configuredProxy) {
        if (request.proxy != null && request.proxy.equals(HttpProxy.NO_PROXY) == false) {
            // if a proxy scheme is configured use this, but fall back to the same than the request in case there was no special
            // configuration given
            String scheme = request.proxy.getScheme() != null ? request.proxy.getScheme().scheme() : Scheme.HTTP.scheme();
            HttpHost proxy = new HttpHost(request.proxy.getHost(), request.proxy.getPort(), scheme);
            config.setProxy(proxy);
        } else if (HttpProxy.NO_PROXY.equals(configuredProxy) == false) {
            HttpHost proxy = new HttpHost(configuredProxy.getHost(), configuredProxy.getPort(), configuredProxy.getScheme().scheme());
            config.setProxy(proxy);
        }
    }

    /**
     * Creates an HTTP proxy from the system wide settings
     *
     * @return An HTTP proxy instance, if no settings are configured this will be an HttpProxy.NO_PROXY instance
     */
    private HttpProxy getProxyFromSettings(Settings settings) {
        String proxyHost = HttpSettings.PROXY_HOST.get(settings);
        Scheme proxyScheme = HttpSettings.PROXY_SCHEME.exists(settings) ?
                Scheme.parse(HttpSettings.PROXY_SCHEME.get(settings)) : Scheme.HTTP;
        int proxyPort = HttpSettings.PROXY_PORT.get(settings);
        if (proxyPort != 0 && Strings.hasText(proxyHost)) {
            logger.info("Using default proxy for http input and slack/hipchat/pagerduty/webhook actions [{}:{}]", proxyHost, proxyPort);
        } else if (proxyPort != 0 ^ Strings.hasText(proxyHost)) {
            throw new IllegalArgumentException("HTTP proxy requires both settings: [" + HttpSettings.PROXY_HOST.getKey() + "] and [" +
                    HttpSettings.PROXY_PORT.getKey() + "]");
        }

        if (proxyPort > 0 && Strings.hasText(proxyHost)) {
            return new HttpProxy(proxyHost, proxyPort, proxyScheme);
        }

        return HttpProxy.NO_PROXY;
    }

    private URI createURI(HttpRequest request) {
        // this could be really simple, as the apache http client has a UriBuilder class, however this class is always doing
        // url path escaping, and we have done this already, so this would result in double escaping
        try {
            List<NameValuePair> qparams = new ArrayList<>(request.params.size());
            request.params.forEach((k, v) -> qparams.add(new BasicNameValuePair(k, v)));
            String format = URLEncodedUtils.format(qparams, "UTF-8");
            URI uri = URIUtils.createURI(request.scheme.scheme(), request.host, request.port, request.path,
                    Strings.isNullOrEmpty(format) ? null : format, null);

            return uri;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
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
