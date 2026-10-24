/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.asn1.x509.GeneralNames;

/**
 * The value of an X.509 <em>Subject Alternative Names</em> extension, as built by
 * {@link CertGenUtils#getSubjectAlternativeNames} or {@link CertGenUtils#getSubjectAlternativeNamesValue} and consumed by the
 * certificate and certificate signing request generation methods of {@link CertGenUtils}.
 * <p>
 * This is an opaque handle for a Bouncy Castle {@code GeneralNames} so that callers outside this module do not depend on
 * Bouncy Castle types, which are shaded into this module's jar.
 * </p>
 */
public final class SubjectAlternativeNames {

    private final GeneralNames generalNames;

    SubjectAlternativeNames(GeneralNames generalNames) {
        this.generalNames = generalNames;
    }

    GeneralNames generalNames() {
        return generalNames;
    }

    @Override
    public String toString() {
        return generalNames.toString();
    }
}
