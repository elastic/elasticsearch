/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;

import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_SEE_OTHER;

class HttpClient {

    /**
     * A PasswordAuthenticationHolder is just a wrapper around a PasswordAuthentication to implement AutoCloseable.
     * This construction makes it possible to use a PasswordAuthentication in a try-with-resources statement, which
     * makes it easier to ensure cleanup of the PasswordAuthentication is performed after it's finished being used.
     */
    static final class PasswordAuthenticationHolder implements AutoCloseable {
        private PasswordAuthentication auth;

        PasswordAuthenticationHolder(String username, char[] passwordChars) {
            this.auth = new PasswordAuthentication(username, passwordChars); // clones the passed-in chars
        }

        public PasswordAuthentication get() {
            Objects.requireNonNull(auth);
            return auth;
        }

        @Override
        public void close() {
            final PasswordAuthentication clear = this.auth;
            this.auth = null; // set to null and then clear it
            Arrays.fill(clear.getPassword(), '\0'); // zero out the password chars
        }
    }

    // a private sentinel value for representing the idea that there's no auth for some request.
    // this allows us to have a not-null requirement on the methods that do accept an auth.
    // if you don't want auth, then don't use those methods. ;)
    private static final PasswordAuthentication NO_AUTH = new PasswordAuthentication("no_auth", "no_auth_unused".toCharArray());

    PasswordAuthentication auth(final String username, final String password) {
        return new PasswordAuthentication(username, password.toCharArray());
    }

    byte[] getBytes(final String url) throws IOException {
        return getBytes(NO_AUTH, url);
    }

    byte[] getBytes(final PasswordAuthentication auth, final String url) throws IOException {
        return get(auth, url).readAllBytes();
    }

    InputStream get(final String url) throws IOException {
        return get(NO_AUTH, url);
    }

    InputStream get(final PasswordAuthentication auth, final String url) throws IOException {
        Objects.requireNonNull(auth);
        Objects.requireNonNull(url);

        final String originalAuthority = new URL(url).getAuthority();

        String innerUrl = url;
        HttpURLConnection conn = createConnection(auth, innerUrl);

        int redirectsCount = 0;
        while (true) {
            switch (conn.getResponseCode()) {
                case HTTP_OK:
                    return getInputStream(conn);
                case HTTP_MOVED_PERM:
                case HTTP_MOVED_TEMP:
                case HTTP_SEE_OTHER:
                    if (redirectsCount++ > 50) {
                        throw new IllegalStateException("too many redirects connection to [" + url + "]");
                    }

                    // deal with redirections (including relative urls)
                    final String location = conn.getHeaderField("Location");
                    final URL base = new URL(innerUrl);
                    final URL next = new URL(base, location);
                    innerUrl = next.toExternalForm();

                    // compare the *original* authority and the next authority to determine whether to include auth details.
                    // this means that the host and port (if it is provided explicitly) are considered. it also means that if we
                    // were to ping-pong back to the original authority, then we'd start including the auth details again.
                    final String nextAuthority = next.getAuthority();
                    if (originalAuthority.equals(nextAuthority)) {
                        conn = createConnection(auth, innerUrl);
                    } else {
                        conn = createConnection(NO_AUTH, innerUrl);
                    }
                    break;
                case HTTP_NOT_FOUND:
                    throw new ResourceNotFoundException("{} not found", url);
                default:
                    int responseCode = conn.getResponseCode();
                    throw new ElasticsearchStatusException("error during downloading {}", RestStatus.fromCode(responseCode), url);
            }
        }
    }

    @SuppressForbidden(reason = "we need socket connection to download data from internet")
    private static InputStream getInputStream(final HttpURLConnection conn) throws IOException {
        return conn.getInputStream();
    }

    private static HttpURLConnection createConnection(final PasswordAuthentication auth, final String url) throws IOException {
        final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        if (auth != NO_AUTH) {
            conn.setAuthenticator(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return auth;
                }
            });
        }
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);
        conn.setDoOutput(false);
        conn.setInstanceFollowRedirects(false);
        return conn;
    }
}
