/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Read-only URL-based blob store
 */
public class URLBlobStore implements BlobStore {

    static final Setting<Integer> HTTP_MAX_RETRIES_SETTING = Setting.intSetting(
        "repositories.uri.http.max_retries",
        HttpClientSettings.DEFAULT_MAX_RETRIES,
        0,
        Integer.MAX_VALUE,
        Setting.Property.NodeScope);

    static final Setting<TimeValue> SOCKET_TIMEOUT_SETTING = Setting.timeSetting(
        "repositories.uri.http.socket_timeout",
        TimeValue.timeValueMillis(HttpClientSettings.DEFAULT_SOCKET_TIMEOUT_MILLIS),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(60),
        Setting.Property.NodeScope);


    private final URL path;

    private final int bufferSizeInBytes;
    private final CheckedFunction<BlobPath, BlobContainer, MalformedURLException> blobContainerFactory;

    /**
     * Constructs new read-only URL-based blob store
     * <p>
     * The following settings are supported
     * <dl>
     * <dt>buffer_size</dt>
     * <dd>- size of the read buffer, defaults to 100KB</dd>
     * </dl>
     *
     * @param settings settings
     * @param path     base URL
     */
    public URLBlobStore(Settings settings, URL path, URLHttpClient httpClient) {
        this.path = path;
        this.bufferSizeInBytes = (int) settings.getAsBytesSize("repositories.uri.buffer_size",
            new ByteSizeValue(100, ByteSizeUnit.KB)).getBytes();
        final HttpClientSettings httpClientSettings = new HttpClientSettings();
        httpClientSettings.setMaxRetries(HTTP_MAX_RETRIES_SETTING.get(settings));
        httpClientSettings.setSocketTimeoutMs((int) SOCKET_TIMEOUT_SETTING.get(settings).millis());

        final String protocol = this.path.getProtocol();
        if (protocol.equals("http") || protocol.equals("https")) {
            this.blobContainerFactory = (blobPath) ->
                new HttpURLBlobContainer(this, blobPath, buildPath(blobPath), httpClient, httpClientSettings);
        } else if (protocol.equals("file")) {
            this.blobContainerFactory = (blobPath) -> new FileURLBlobContainer(this, blobPath, buildPath(blobPath));
        } else {
            this.blobContainerFactory = (blobPath) -> new URLBlobContainer(this, blobPath, buildPath(blobPath));
        }
    }

    @Override
    public String toString() {
        return path.toString();
    }

    /**
     * Returns base URL
     *
     * @return base URL
     */
    public URL path() {
        return path;
    }

    /**
     * Returns read buffer size
     *
     * @return read buffer size
     */
    public int bufferSizeInBytes() {
        return this.bufferSizeInBytes;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        try {
            return blobContainerFactory.apply(path);
        } catch (MalformedURLException ex) {
            throw new BlobStoreException("malformed URL " + path, ex);
        }
    }

    @Override
    public void close() {
        // nothing to do here...
    }

    /**
     * Builds URL using base URL and specified path
     *
     * @param path relative path
     * @return Base URL + path
     */
    private URL buildPath(BlobPath path) throws MalformedURLException {
        String[] paths = path.toArray();
        if (paths.length == 0) {
            return path();
        }
        URL blobPath = new URL(this.path, paths[0] + "/");
        if (paths.length > 1) {
            for (int i = 1; i < paths.length; i++) {
                blobPath = new URL(blobPath, paths[i] + "/");
            }
        }
        return blobPath;
    }
}
