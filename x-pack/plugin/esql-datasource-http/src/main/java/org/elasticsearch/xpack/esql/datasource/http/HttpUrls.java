/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

/**
 * Renders an HTTP location for an error message. {@link StoragePath#toString()} is the URL verbatim, so a
 * pre-signed URL's signature and any {@code user:pass@} would be echoed to the user; every message in this
 * module that names the object goes through {@link #redact} instead.
 */
final class HttpUrls {

    private HttpUrls() {}

    /** {@code scheme://host[:port]/path}, without user info, query string or fragment. */
    static String redact(StoragePath path) {
        StringBuilder url = new StringBuilder(path.scheme()).append(StoragePath.SCHEME_SEPARATOR).append(path.host());
        if (path.port() > 0) {
            url.append(StoragePath.PORT_SEPARATOR).append(path.port());
        }
        return url.append(path.path()).toString();
    }
}
