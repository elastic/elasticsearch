/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Im memory representation of the trusted names for a "trust group".
 *
 * @see RestrictedTrustManager
 */
class CertificateTrustRestrictions {

    private final Set<Predicate<String>> trustedNames;

    CertificateTrustRestrictions(Collection<String> trustedNames) {
        this.trustedNames = trustedNames.stream().map(Automatons::predicate).collect(Collectors.toSet());
    }

    /**
     * @return The names (X509 certificate subjectAlternateNames) of the nodes that are
     * allowed to connect to this cluster (for the targeted interface) .
     */
    Set<Predicate<String>> getTrustedNames() {
        return Collections.unmodifiableSet(trustedNames);
    }

    @Override
    public String toString() {
        return "{trustedNames=" + trustedNames + '}';
    }
}
