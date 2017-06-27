/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.net.client.jre;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.elasticsearch.xpack.sql.net.client.ClientException;
import org.elasticsearch.xpack.sql.net.client.ConnectionConfiguration;
import org.elasticsearch.xpack.sql.net.client.DataOutputConsumer;
import org.elasticsearch.xpack.sql.net.client.util.Bytes;
import org.elasticsearch.xpack.sql.net.client.util.IOUtils;

public class JreHttpUrlConnection implements Closeable {

    private boolean closed = false;
    final HttpURLConnection con;
    private final URL url;

    public JreHttpUrlConnection(URL url, ConnectionConfiguration cfg) throws ClientException {
        this.url = url;
        try {
            con = (HttpURLConnection) url.openConnection();
        } catch (IOException ex) {
            throw new ClientException(ex, "Cannot setup connection to %s", url);
        }

        con.setConnectTimeout((int) cfg.getConnectTimeout());
        con.setReadTimeout((int) cfg.getNetworkTimeout());

        con.setAllowUserInteraction(false);
        con.setUseCaches(false);
        // HttpURL adds this header by default, HttpS does not
        // adding it here to be consistent
        con.setRequestProperty("Accept-Charset", "UTF-8");
        // NOCOMMIT if we're going to accept gzip then we need to transparently unzip it on the way out...
        // con.setRequestProperty("Accept-Encoding", "gzip");
    }

    public boolean head() throws ClientException {
        try {
            con.setRequestMethod("HEAD");
            int responseCode = con.getResponseCode();
            return responseCode == HttpURLConnection.HTTP_OK;
        } catch (IOException ex) {
            throw new ClientException(ex, "Cannot HEAD address %s", url);
        }
    }

    public Bytes put(DataOutputConsumer doc) throws ClientException { // NOCOMMIT why is this called put when it is a post?
        try {
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");
            try (OutputStream out = con.getOutputStream()) {
                doc.accept(new DataOutputStream(out));
            }
            if (con.getResponseCode() >= 400) {
                InputStream err = con.getErrorStream();
                String response;
                if (err == null) {
                    response = "server did not return a response";
                } else {
                    // NOCOMMIT figure out why this returns weird characters. Can reproduce with unauthorized.
                    response = new String(IOUtils.asBytes(err).bytes(), StandardCharsets.UTF_8);
                }
                throw new ClientException("Protocol/client error; server returned [" + con.getResponseMessage() + "]: " + response);
            }
            // NOCOMMIT seems weird that we buffer this into a byte stream and then wrap it in a byte array input stream.....
            return IOUtils.asBytes(con.getInputStream());
        } catch (IOException ex) {
            throw new ClientException(ex, "Cannot POST address %s", url);
        }
    }

    public void connect() {
        if (closed) {
            throw new ClientException("Connection cannot be reused");
        }
        try {
            con.connect();
        } catch (IOException ex) {
            throw new ClientException(ex, "Cannot open connection to %s", url);
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

    //
    // main call class
    //

    public static <R> R http(URL url, ConnectionConfiguration cfg, Function<JreHttpUrlConnection, R> handler) {
        try (JreHttpUrlConnection con = new JreHttpUrlConnection(url, cfg)) {
            return handler.apply(con);
        }
    }
}