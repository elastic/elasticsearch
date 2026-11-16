/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli.bc;

import java.util.HashMap;
import java.util.Map;

/**
 * A key usage bit in the X.509 Key Usage extension.
 * <p>
 * Callers pass CLI names to {@link CertGenUtils#buildKeyUsage(java.util.Collection)}, which resolves them to a set of these values.
 * The Bouncy Castle bit values are not exposed.
 * </p>
 */
public enum KeyUsage {
    DIGITAL_SIGNATURE("digitalSignature", org.bouncycastle.asn1.x509.KeyUsage.digitalSignature),
    NON_REPUDIATION("nonRepudiation", org.bouncycastle.asn1.x509.KeyUsage.nonRepudiation),
    KEY_ENCIPHERMENT("keyEncipherment", org.bouncycastle.asn1.x509.KeyUsage.keyEncipherment),
    DATA_ENCIPHERMENT("dataEncipherment", org.bouncycastle.asn1.x509.KeyUsage.dataEncipherment),
    KEY_AGREEMENT("keyAgreement", org.bouncycastle.asn1.x509.KeyUsage.keyAgreement),
    KEY_CERT_SIGN("keyCertSign", org.bouncycastle.asn1.x509.KeyUsage.keyCertSign),
    CRL_SIGN("cRLSign", org.bouncycastle.asn1.x509.KeyUsage.cRLSign),
    ENCIPHER_ONLY("encipherOnly", org.bouncycastle.asn1.x509.KeyUsage.encipherOnly),
    DECIPHER_ONLY("decipherOnly", org.bouncycastle.asn1.x509.KeyUsage.decipherOnly);

    private static final Map<String, KeyUsage> BY_NAME;

    static {
        Map<String, KeyUsage> byName = new HashMap<>();
        for (KeyUsage usage : values()) {
            byName.put(usage.usageName, usage);
        }
        BY_NAME = Map.copyOf(byName);
    }

    private final String usageName;
    private final int bits;

    KeyUsage(String usageName, int bits) {
        this.usageName = usageName;
        this.bits = bits;
    }

    String usageName() {
        return usageName;
    }

    /**
     * @return the key usage for this CLI name, or {@code null} if {@code name} is {@code null} or not a known key usage. The match is
     *         case-sensitive. Leading and trailing whitespace is ignored.
     */
    static KeyUsage fromName(String name) {
        if (name == null) {
            return null;
        }
        return BY_NAME.get(name.trim());
    }

    int bits() {
        return bits;
    }
}
