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
public record StoredCertificate(
    X509Certificate certificate,
    @Nullable String path,
    String format,
    @Nullable String alias,
    boolean hasPrivateKey
) {
    public StoredCertificate {
        Objects.requireNonNull(certificate, "Certificate may not be null");
        Objects.requireNonNull(format, "Format may not be null");
    }
}
