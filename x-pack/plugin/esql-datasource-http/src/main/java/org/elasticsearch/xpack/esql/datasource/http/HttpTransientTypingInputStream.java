/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps an HTTP response-body stream so a fault <em>while reading the body</em> surfaces as a typed
 * {@link ExternalUnavailableException} the provider-agnostic resume loop can act on — matching the
 * S3 / GCS / Azure mid-read typing wrappers.
 * <p>
 * The JDK {@code HttpClient} body stream surfaces a mid-read drop in two shapes. A TCP reset throws a
 * {@link java.net.SocketException}, which the resume loop's classifier already treats as transient. But a clean
 * close that delivers fewer bytes than the declared {@code Content-Length} — the idle-drop / premature-EOF case
 * this PR targets — throws a <b>plain {@link IOException}</b> ({@code "fixed content-length: N, bytes received: M"}),
 * which the classifier treats as a hard error, so the resume would never engage. We re-type any mid-read
 * {@code IOException} here: the body is opaque object bytes, so a mid-read fault is a transport fault and is
 * always transient. Unlike the cloud providers there is no status on a mid-body fault (the response status was
 * read at open), so it is never flagged throttling.
 */
final class HttpTransientTypingInputStream extends FilterInputStream {

    private final StoragePath path;

    HttpTransientTypingInputStream(InputStream delegate, StoragePath path) {
        super(delegate);
        this.path = path;
    }

    @Override
    public int read() throws IOException {
        try {
            return in.read();
        } catch (IOException e) {
            throw type(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            return in.read(b, off, len);
        } catch (IOException e) {
            throw type(e);
        }
    }

    private ExternalUnavailableException type(IOException e) {
        return new ExternalUnavailableException(false, e, "transient read failure for [{}]", path);
    }
}
