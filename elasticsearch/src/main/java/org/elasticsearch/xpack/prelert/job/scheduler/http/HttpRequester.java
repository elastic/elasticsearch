/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler.http;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;


/**
 * Executes requests from an HTTP or HTTPS URL by sending a request body.
 * HTTP or HTTPS is deduced from the supplied URL.
 * Invalid certificates are tolerated for HTTPS access, similar to "curl -k".
 */
public class HttpRequester {

    private static final Logger LOGGER = Loggers.getLogger(HttpRequester.class);

    private static final String TLS = "TLS";
    private static final String GET = "GET";
    private static final String DELETE = "DELETE";
    private static final String AUTH_HEADER = "Authorization";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final SSLSocketFactory TRUSTING_SOCKET_FACTORY;
    private static final HostnameVerifier TRUSTING_HOSTNAME_VERIFIER;

    private static final int CONNECT_TIMEOUT_MILLIS = 30000;
    private static final int READ_TIMEOUT_MILLIS = 600000;

    static {
        SSLSocketFactory trustingSocketFactory = null;
        try {
            SSLContext sslContext = SSLContext.getInstance(TLS);
            sslContext.init(null, new TrustManager[]{ new NoOpTrustManager() }, null);
            trustingSocketFactory = sslContext.getSocketFactory();
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            LOGGER.warn("Unable to set up trusting socket factory", e);
        }

        TRUSTING_SOCKET_FACTORY = trustingSocketFactory;
        TRUSTING_HOSTNAME_VERIFIER = new NoOpHostnameVerifier();
    }

    private final String authHeader;
    private final String contentTypeHeader;

    public HttpRequester() {
        this(null);
    }

    public HttpRequester(String authHeader) {
        this(authHeader, null);
    }

    public HttpRequester(String authHeader, String contentTypeHeader) {
        this.authHeader = authHeader;
        this.contentTypeHeader = contentTypeHeader;
    }

    public HttpResponse get(String url, String requestBody) throws IOException {
        return request(url, requestBody, GET);
    }

    public HttpResponse delete(String url, String requestBody) throws IOException    {
        return request(url, requestBody, DELETE);
    }

    private HttpResponse request(String url, String requestBody, String method) throws IOException {
        URL urlObject = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) urlObject.openConnection();
        connection.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
        connection.setReadTimeout(READ_TIMEOUT_MILLIS);

        // TODO: we could add a config option to allow users who want to
        // rigorously enforce valid certificates to do so
        if (connection instanceof HttpsURLConnection) {
            // This is the equivalent of "curl -k", i.e. tolerate connecting to
            // an Elasticsearch with a self-signed certificate or a certificate
            // that doesn't match its hostname.
            HttpsURLConnection httpsConnection = (HttpsURLConnection)connection;
            if (TRUSTING_SOCKET_FACTORY != null) {
                httpsConnection.setSSLSocketFactory(TRUSTING_SOCKET_FACTORY);
            }
            httpsConnection.setHostnameVerifier(TRUSTING_HOSTNAME_VERIFIER);
        }
        connection.setRequestMethod(method);
        if (authHeader != null) {
            connection.setRequestProperty(AUTH_HEADER, authHeader);
        }
        if (contentTypeHeader != null) {
            connection.setRequestProperty(CONTENT_TYPE_HEADER, contentTypeHeader);
        }
        if (requestBody != null) {
            connection.setDoOutput(true);
            writeRequestBody(requestBody, connection);
        }
        if (connection.getResponseCode() != HttpResponse.OK_STATUS) {
            return new HttpResponse(connection.getErrorStream(), connection.getResponseCode());
        }
        return new HttpResponse(connection.getInputStream(), connection.getResponseCode());
    }

    private static void writeRequestBody(String requestBody, HttpURLConnection connection) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(connection.getOutputStream());
        dataOutputStream.writeBytes(requestBody);
        dataOutputStream.flush();
        dataOutputStream.close();
    }

    /**
     * Hostname verifier that ignores hostname discrepancies.
     */
    private static final class NoOpHostnameVerifier implements HostnameVerifier {
        @Override
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    /**
     * Certificate trust manager that ignores certificate issues.
     */
    private static final class NoOpTrustManager implements X509TrustManager {
        private static final X509Certificate[] EMPTY_CERTIFICATE_ARRAY = new X509Certificate[0];

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            // Ignore certificate problems
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            // Ignore certificate problems
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return EMPTY_CERTIFICATE_ARRAY;
        }
    }
}
