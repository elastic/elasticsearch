/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.core.Nullable;

import java.security.cert.X509Certificate;
import java.util.Objects;

/**
 * Information about a certificate that is locally stored.It includes a reference to the {@link X509Certificate} itself,
 * as well as information about where it was loaded from.
 */
public final class StoredCertificate {

    private final X509Certificate certificate;

    @Nullable
    // Will be null in PKCS#11
    private final String path;

    private final String format;

    @Nullable
    // Will be null in PEM
    private final String alias;

    private final boolean hasPrivateKey;

    public StoredCertificate(X509Certificate certificate, String path, String format, String alias, boolean hasPrivateKey) {
        this.certificate = Objects.requireNonNull(certificate, "Certificate may not be null");
        this.path = path;
        this.format = Objects.requireNonNull(format, "Format may not be null");
        this.alias = alias;
        this.hasPrivateKey = hasPrivateKey;
    }

    public X509Certificate getCertificate() {
        return certificate;
    }

    public String getPath() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoredCertificate that = (StoredCertificate) o;
        return hasPrivateKey == that.hasPrivateKey
            && certificate.equals(that.certificate)
            && Objects.equals(path, that.path)
            && format.equals(that.format)
            && Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(certificate, path, format, alias, hasPrivateKey);
    }
}
