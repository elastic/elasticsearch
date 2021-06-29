/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLClientInfoException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.util.Base64;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.sql.rowset.serial.SerialException;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.sql.client.UriUtils.appendSegmentToPath;
import static org.elasticsearch.xpack.sql.proto.Protocol.SQL_QUERY_REST_ENDPOINT;

/**
 * Low-level http client using the built-in {@link HttpURLConnection}.
 * As such, it has a stateless, on-demand, request-response flow without
 * any connection pooling or sharing.
 */
public class JreHttpUrlConnection implements Closeable {
    /**
     * State added to {@link SQLException}s when the server encounters an
     * error.
     */
    public static final String SQL_STATE_BAD_SERVER = "bad_server";
    private static final String SQL_NOT_AVAILABLE_ERROR_MESSAGE = "Incorrect HTTP method for uri [" + SQL_QUERY_REST_ENDPOINT
            + "?error_trace] and method [POST], allowed:";

    public static <R> R http(String path, String query, ConnectionConfiguration cfg, Function<JreHttpUrlConnection, R> handler) {
        final URI uriPath = appendSegmentToPath(cfg.baseUri(), path);  // update path if needed
        final String uriQuery = query == null ? uriPath.getQuery() : query; // update query if needed
        final URL url;
        try {
            url = new URI(uriPath.getScheme(), null, uriPath.getHost(), uriPath.getPort(), uriPath.getPath(), uriQuery,
                    uriPath.getFragment()).toURL();
        } catch (URISyntaxException | MalformedURLException ex) {
            throw new ClientException("Cannot build url using base: [" + uriPath + "] query: [" + query + "] path: [" + path + "]", ex);
        }
        try (JreHttpUrlConnection con = new JreHttpUrlConnection(url, cfg)) {
            return handler.apply(con);
        }
    }

    private boolean closed = false;
    final HttpURLConnection con;
    private final URL url;
    private static final String GZIP = "gzip";

    public JreHttpUrlConnection(URL url, ConnectionConfiguration cfg) throws ClientException {
        this.url = url;
        try {
            // due to the way the URL API is designed, the proxy needs to be passed in first
            Proxy p = cfg.proxyConfig().proxy();
            con = (HttpURLConnection) (p != null ? url.openConnection(p) : url.openConnection());
        } catch (IOException ex) {
            throw new ClientException("Cannot setup connection to " + url + " (" + ex.getMessage() + ")", ex);
        }

        // the rest of the connection setup
        setupConnection(cfg);
    }

    private void setupConnection(ConnectionConfiguration cfg) {
        // setup basic stuff first

        // timeouts
        con.setConnectTimeout((int) cfg.connectTimeout());
        con.setReadTimeout((int) cfg.networkTimeout());

        // disable content caching
        con.setAllowUserInteraction(false);
        con.setUseCaches(false);

        // HTTP params
        // HttpURL adds this header by default, HttpS does not
        // adding it here to be consistent
        con.setRequestProperty("Accept-Charset", "UTF-8");
        //con.setRequestProperty("Accept-Encoding", GZIP);

        setupSSL(cfg);
        setupBasicAuth(cfg);
    }

    private void setupSSL(ConnectionConfiguration cfg) {
        if (cfg.sslConfig().isEnabled()) {
            HttpsURLConnection https = (HttpsURLConnection) con;
            SSLSocketFactory factory = cfg.sslConfig().sslSocketFactory();
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                https.setSSLSocketFactory(factory);
                return null;
            });
        }
    }

    private void setupBasicAuth(ConnectionConfiguration cfg) {
        if (StringUtils.hasText(cfg.authUser())) {
            String basicValue = cfg.authUser() + ":" + cfg.authPass();
            String encoded = StringUtils.asUTFString(Base64.getEncoder().encode(StringUtils.toUTF(basicValue)));
            con.setRequestProperty("Authorization", "Basic " + encoded);
        }
    }

    public boolean head() throws ClientException {
        try {
            con.setRequestMethod("HEAD");
            int responseCode = con.getResponseCode();
            return responseCode == HttpURLConnection.HTTP_OK;
        } catch (IOException ex) {
            throw new ClientException("Cannot HEAD address " + url + " (" + ex.getMessage() + ")", ex);
        }
    }

    public <R> ResponseOrException<R> request(
            CheckedConsumer<OutputStream, IOException> doc,
            CheckedBiFunction<InputStream, Function<String, String>, R, IOException> parser,
            String requestMethod
    ) throws ClientException {
        return request(doc, parser, requestMethod, "application/json");
    }

    public <R> ResponseOrException<R> request(
            CheckedConsumer<OutputStream, IOException> doc,
            CheckedBiFunction<InputStream, Function<String, String>, R, IOException> parser,
            String requestMethod, String contentTypeHeader
    ) throws ClientException {
        try {
            con.setRequestMethod(requestMethod);
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", contentTypeHeader);
            con.setRequestProperty("Accept", "application/json");
            if (doc != null) {
                try (OutputStream out = con.getOutputStream()) {
                    doc.accept(out);
                }
            }
            if (shouldParseBody(con.getResponseCode())) {
                try (InputStream stream = getStream(con, con.getInputStream())) {
                    return new ResponseOrException<>(parser.apply(
                            new BufferedInputStream(stream),
                            con::getHeaderField
                            ));
                }
            }
            return parserError();
        } catch (IOException ex) {
            throw new ClientException("Cannot POST address " + url + " (" + ex.getMessage() + ")", ex);
        }
    }

    private boolean shouldParseBody(int responseCode) {
        return responseCode == 200 || responseCode == 201 || responseCode == 202;
    }

    private <R> ResponseOrException<R> parserError() throws IOException {
        RemoteFailure failure;
        try (InputStream stream = getStream(con, con.getErrorStream())) {
            failure = RemoteFailure.parseFromResponse(stream);
        }
        if (con.getResponseCode() >= 500) {
            return new ResponseOrException<>(new SQLException("Server encountered an error ["
                    + failure.reason() + "]. [" + failure.remoteTrace() + "]", SQL_STATE_BAD_SERVER));
        }
        SqlExceptionType type = SqlExceptionType.fromRemoteFailureType(failure.type());
        if (type == null) {
            // check if x-pack or sql are not available (x-pack not installed or sql not enabled)
            // by checking the error message the server is sending back
            if (con.getResponseCode() >= HttpURLConnection.HTTP_BAD_REQUEST && failure.reason().contains(SQL_NOT_AVAILABLE_ERROR_MESSAGE)) {
                return new ResponseOrException<>(new SQLException("X-Pack/SQL does not seem to be available"
                        + " on the Elasticsearch node using the access path '"
                        + con.getURL().getHost()
                        + (con.getURL().getPort() > 0 ? ":" + con.getURL().getPort() : "")
                        + "'."
                        + " Please verify X-Pack is installed and SQL enabled. Alternatively, check if any proxy is interfering"
                        + " the communication to Elasticsearch",
                        SQL_STATE_BAD_SERVER));
            }
            return new ResponseOrException<>(new SQLException("Server sent bad type ["
                    + failure.type() + "]. Original type was [" + failure.reason() + "]. ["
                    + failure.remoteTrace() + "]", SQL_STATE_BAD_SERVER));
        }
        return new ResponseOrException<>(type.asException(failure.reason()));
    }

    public static class ResponseOrException<R> {
        private final R response;
        private final SQLException exception;

        private ResponseOrException(R response) {
            this.response = response;
            this.exception = null;
        }

        private ResponseOrException(SQLException exception) {
            this.response = null;
            this.exception = exception;
        }

        public R getResponseOrThrowException() throws SQLException {
            if (exception != null) {
                throw exception;
            }
            assert response != null;
            return response;
        }
    }

    private static InputStream getStream(HttpURLConnection con, InputStream stream) throws IOException {
        if (GZIP.equals(con.getContentEncoding())) {
            return new GZIPInputStream(stream);
        }
        return stream;
    }

    public void connect() {
        if (closed) {
            throw new ClientException("Connection cannot be reused");
        }
        try {
            con.connect();
        } catch (IOException ex) {
            throw new ClientException("Cannot open connection to " + url + " (" + ex.getMessage() + ")", ex);
        }
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;

            // consume streams
            consumeStreams();
        }
    }

    public void disconnect() {
        try {
            connect();
        } finally {
            con.disconnect();
        }
    }

    // http://docs.oracle.com/javase/7/docs/technotes/guides/net/http-keepalive.html
    private void consumeStreams() {
        try (InputStream in = con.getInputStream()) {
            while (in != null && in.read() > -1) {
            }
        } catch (IOException ex) {
            // ignore
        } finally {
            try (InputStream ein = con.getErrorStream()) {
                while (ein != null && ein.read() > -1) {
                }
            } catch (IOException ex) {
                // keep on ignoring
            }
        }
    }

    /**
     * Exception type.
     */
    public enum SqlExceptionType {
        UNKNOWN(SQLException::new),
        SERIAL(SerialException::new),
        CLIENT_INFO(message -> new SQLClientInfoException(message, emptyMap())),
        DATA(SQLDataException::new),
        SYNTAX(SQLSyntaxErrorException::new),
        RECOVERABLE(SQLRecoverableException::new),
        TIMEOUT(SQLTimeoutException::new),
        SECURITY(SQLInvalidAuthorizationSpecException::new),
        NOT_SUPPORTED(SQLFeatureNotSupportedException::new);

        public static SqlExceptionType fromRemoteFailureType(String type) {
            switch (type) {
                case "analysis_exception":
                case "resource_not_found_exception":
                case "verification_exception":
                    return DATA;
                case "planning_exception":
                case "mapping_exception":
                    return NOT_SUPPORTED;
                case "parsing_exception":
                    return SYNTAX;
                case "security_exception":
                    return SECURITY;
                case "timeout_exception":
                    return TIMEOUT;
                default:
                    return null;
            }
        }

        private final Function<String, SQLException> toException;

        SqlExceptionType(Function<String, SQLException> toException) {
            this.toException = toException;
        }

        SQLException asException(String message) {
            if (message == null) {
                throw new IllegalArgumentException("[message] cannot be null");
            }
            return toException.apply(message);
        }
    }
}
