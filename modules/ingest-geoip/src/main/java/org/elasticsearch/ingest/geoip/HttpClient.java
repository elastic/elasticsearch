/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_SEE_OTHER;

class HttpClient {

    byte[] getBytes(String url) throws IOException {
        return get(url).readAllBytes();
    }

    InputStream get(String urlToGet) throws IOException {
        return doPrivileged(() -> {
            String url = urlToGet;
            HttpURLConnection conn = createConnection(url);

            int redirectsCount = 0;
            while (true) {
                switch (conn.getResponseCode()) {
                    case HTTP_OK:
                        return new BufferedInputStream(getInputStream(conn));
                    case HTTP_MOVED_PERM:
                    case HTTP_MOVED_TEMP:
                    case HTTP_SEE_OTHER:
                        if (redirectsCount++ > 50) {
                            throw new IllegalStateException("too many redirects connection to [" + urlToGet + "]");
                        }
                        String location = conn.getHeaderField("Location");
                        URL base = new URL(url);
                        URL next = new URL(base, location);  // Deal with relative URLs
                        url = next.toExternalForm();
                        conn = createConnection(url);
                        break;
                    case HTTP_NOT_FOUND:
                        throw new ResourceNotFoundException("{} not found", urlToGet);
                    default:
                        int responseCode = conn.getResponseCode();
                        throw new ElasticsearchStatusException("error during downloading {}", RestStatus.fromCode(responseCode), urlToGet);
                }
            }
        });
    }

    @SuppressForbidden(reason = "we need socket connection to download data from internet")
    private InputStream getInputStream(HttpURLConnection conn) throws IOException {
        return conn.getInputStream();
    }

    private HttpURLConnection createConnection(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);
        conn.setDoOutput(false);
        conn.setInstanceFollowRedirects(false);
        return conn;
    }

    private static <R> R doPrivileged(CheckedSupplier<R, IOException> supplier) throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<R>) supplier::get);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }
}
