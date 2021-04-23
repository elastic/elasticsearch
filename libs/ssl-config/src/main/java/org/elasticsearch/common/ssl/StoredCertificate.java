/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import java.nio.file.Path;
import java.security.cert.X509Certificate;

/**
 * Information about a certificate that is locally stored.It includes a reference to the {@link X509Certificate} itself,
 * as well as information about where it was loaded from.
 */
public class StoredCertificate {

    private final X509Certificate certificate;
    private final Path path;
    private final String format;
    private final String alias;
    private final boolean hasPrivateKey;

    public StoredCertificate(X509Certificate certificate, Path path, String format, String alias, boolean hasPrivateKey) {
        this.certificate = certificate;
        this.path = path;
        this.format = format;
        this.alias = alias;
        this.hasPrivateKey = hasPrivateKey;
    }

    public X509Certificate getCertificate() {
        return certificate;
    }

    public Path getPath() {
        return path;
    }

    public String getFormat() {
        return format;
    }

    public String getAlias() {
        return alias;
    }

    public boolean hasPrivateKey() {
        return hasPrivateKey;
    }
}
