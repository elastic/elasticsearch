/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client.shared;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
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

import javax.sql.rowset.serial.SerialException;

import static java.util.Collections.emptyMap;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

public class JreHttpUrlConnection implements Closeable {
    /**
     * State added to {@link SQLException}s when the server encounters an
     * error.
     */
    public static final String SQL_STATE_BAD_SERVER = "bad_server";

    public static <R> R http(URL url, ConnectionConfiguration cfg, Function<JreHttpUrlConnection, R> handler) {
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
            throw new ClientException(ex, "Cannot setup connection to %s (%s)", url, ex.getMessage());
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
            throw new ClientException(ex, "Cannot HEAD address %s (%s)", url, ex.getMessage());
        }
    }

    public <R> ResponseOrException<R> post(
                CheckedConsumer<DataOutput, IOException> doc,
                CheckedFunction<DataInput, R, IOException> parser
            ) throws ClientException {
        try {
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");
            try (OutputStream out = con.getOutputStream()) {
                doc.accept(new DataOutputStream(out));
            }
            if (con.getResponseCode() < 300) {
                try (InputStream stream = getStream(con, con.getInputStream())) {
                    return new ResponseOrException<>(parser.apply(new DataInputStream(stream)));
                }
            }
            RemoteFailure failure;
            try (InputStream stream = getStream(con, con.getErrorStream())) {
                failure = RemoteFailure.parseFromResponse(stream);
            }
            if (con.getResponseCode() >= 500) {
                /*
                 * Borrowing a page from the HTTP spec, we throw a "transient"
                 * exception if the server responded with a 500, not because
                 * we think that the application should retry, but because we
                 * think that the failure is not the fault of the application.
                 */
                return new ResponseOrException<>(new SQLException("Server encountered an error ["
                    + failure.reason() + "]. [" + failure.remoteTrace() + "]", SQL_STATE_BAD_SERVER));
            }
            SqlExceptionType type = SqlExceptionType.fromRemoteFailureType(failure.type());
            if (type == null) {
                return new ResponseOrException<>(new SQLException("Server sent bad type ["
                    + failure.type() + "]. Original type was [" + failure.reason() + "]. ["
                    + failure.remoteTrace() + "]", SQL_STATE_BAD_SERVER));
            }
            return new ResponseOrException<>(type.asException(failure.reason()));
        } catch (IOException ex) {
            throw new ClientException(ex, "Cannot POST address %s (%s)", url, ex.getMessage());
        }
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
            throw new ClientException(ex, "Cannot open connection to %s (%s)", url, ex.getMessage());
        }
    }

    @Override
    public void close() {
        if (!closed) {
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
