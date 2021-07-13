/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url.http;

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.url.URLBlobContainer;
import org.elasticsearch.common.blobstore.url.URLBlobStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class HttpURLBlobContainer extends URLBlobContainer {
    private final URLHttpClient httpClient;
    private final URLHttpClientSettings httpClientSettings;

    public HttpURLBlobContainer(URLBlobStore blobStore,
                                BlobPath blobPath,
                                URL path,
                                URLHttpClient httpClient,
                                URLHttpClientSettings httpClientSettings) {
        super(blobStore, blobPath, path);
        this.httpClient = httpClient;
        this.httpClientSettings = httpClientSettings;
    }

    @Override
    public InputStream readBlob(String name, long position, long length) throws IOException {
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        }

        return new RetryingHttpInputStream(name,
            getURIForBlob(name),
            position,
            Math.addExact(position, length) - 1,
            httpClient,
            httpClientSettings.getMaxRetries());
    }

    @Override
    public InputStream readBlob(String name) throws IOException {
        return new RetryingHttpInputStream(name, getURIForBlob(name), httpClient, httpClientSettings.getMaxRetries());
    }

    private URI getURIForBlob(String name) throws IOException {
        try {
            return new URL(path, name).toURI();
        } catch (Exception e) {
            throw new IOException("Unable to get an URI for blob with name [" + name + "]", e);
        }
    }
}
