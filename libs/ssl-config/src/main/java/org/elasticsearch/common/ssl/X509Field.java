/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.ssl;

import java.util.EnumSet;
import java.util.stream.Collectors;

/**
 * An enumeration for referencing parts of an X509 certificate by a canonical string value.
 */
public enum X509Field {

    SAN_OTHERNAME_COMMONNAME("subjectAltName.otherName.commonName", true),
    SAN_DNS("subjectAltName.dnsName", true);

    private final String configValue;
    private final boolean supportedForRestrictedTrust;

    X509Field(String configValue, boolean supportedForRestrictedTrust) {
        this.configValue = configValue;
        this.supportedForRestrictedTrust = supportedForRestrictedTrust;
    }

    @Override
    public String toString() {
        return configValue;
    }

    public static X509Field parseForRestrictedTrust(String s) {
        return EnumSet.allOf(X509Field.class)
            .stream()
            .filter(v -> v.supportedForRestrictedTrust)
            .filter(v -> v.configValue.equalsIgnoreCase(s))
            .findFirst()
            .orElseThrow(() -> {
                throw new SslConfigException(
                    s
                        + " is not a supported x509 field for trust restrictions. "
                        + "Recognised values are ["
                        + EnumSet.allOf(X509Field.class).stream().map(e -> e.configValue).collect(Collectors.toSet())
                        + "]"
                );
            });
    }
}
