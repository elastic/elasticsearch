/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.DERTaggedObject;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The entries of an X.509 <em>Subject Alternative Names</em> extension, expressed without any Bouncy Castle types so that callers
 * outside this package never depend on the (relocated) Bouncy Castle API.
 * <p>
 * Internally this is the same {@code Set<GeneralName>} that the tools used to build inline: entries are de-duplicated on their
 * ASN.1 encoding and have no defined order.
 */
public final class SubjectAlternativeNames {

    private static final String CN_OID = "2.5.4.3";

    private final Set<GeneralName> generalNames;

    private SubjectAlternativeNames(Set<GeneralName> generalNames) {
        this.generalNames = Set.copyOf(generalNames);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience factory for pre-collected name lists.
     *
     * @return the subject alternative names, or {@code null} if all lists are empty (i.e. no SAN extension should be added)
     */
    public static SubjectAlternativeNames of(List<String> ipAddresses, List<String> dnsNames, List<String> commonNames) {
        final Builder builder = builder();
        for (String ip : ipAddresses) {
            builder.addIpAddress(ip);
        }
        for (String dns : dnsNames) {
            builder.addDnsName(dns);
        }
        for (String cn : commonNames) {
            builder.addCommonName(cn);
        }
        if (builder.generalNames.isEmpty()) {
            return null;
        }
        return builder.build();
    }

    public int size() {
        return generalNames.size();
    }

    GeneralNames toGeneralNames() {
        return new GeneralNames(generalNames.toArray(new GeneralName[0]));
    }

    /**
     * Creates an X.509 {@link GeneralName} for use as a <em>Common Name</em> in the certificate's <em>Subject Alternative Names</em>
     * extension. A <em>common name</em> is a name with a tag of {@link GeneralName#otherName OTHER}, with an object-id that references
     * the {@link #CN_OID cn} attribute, an explicit tag of '0', and a DER encoded UTF8 string for the name.
     * This usage of using the {@code cn} OID as a <em>Subject Alternative Name</em> is <strong>non-standard</strong> and will not be
     * recognised by other X.509/TLS implementations.
     */
    private static GeneralName createCommonName(String cn) {
        final ASN1Encodable[] sequence = { new ASN1ObjectIdentifier(CN_OID), new DERTaggedObject(true, 0, new DERUTF8String(cn)) };
        return new GeneralName(GeneralName.otherName, new DERSequence(sequence));
    }

    @Override
    public String toString() {
        return generalNames.toString();
    }

    public static final class Builder {
        private final Set<GeneralName> generalNames = new HashSet<>();

        private Builder() {}

        public Builder addIpAddress(String ipAddress) {
            generalNames.add(new GeneralName(GeneralName.iPAddress, ipAddress));
            return this;
        }

        public Builder addDnsName(String dnsName) {
            generalNames.add(new GeneralName(GeneralName.dNSName, dnsName));
            return this;
        }

        public Builder addCommonName(String commonName) {
            generalNames.add(createCommonName(commonName));
            return this;
        }

        public SubjectAlternativeNames build() {
            return new SubjectAlternativeNames(generalNames);
        }
    }
}
