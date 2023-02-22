/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ssl.PemUtils;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.KeyDescriptor;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorBuilder;
import org.opensaml.saml.saml2.metadata.impl.IDPSSODescriptorBuilder;
import org.opensaml.saml.saml2.metadata.impl.KeyDescriptorBuilder;
import org.opensaml.security.credential.UsageType;
import org.opensaml.xmlsec.keyinfo.KeyInfoSupport;
import org.opensaml.xmlsec.signature.KeyInfo;
import org.opensaml.xmlsec.signature.impl.KeyInfoBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivilegedActionException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * This class is designed to support building IdP Metadata in test scenarios.
 * It currently exists in a specific SAML project but could be moved to a general testing location is necessary
 */
public class SamlIdpMetadataBuilder {

    private static final Logger logger = LogManager.getLogger(SamlIdpMetadataBuilder.class);

    private boolean wantSignedAuthnRequests;
    private List<X509Certificate> signingCertificates;
    private String entityId;

    public SamlIdpMetadataBuilder() {
        try {
            SamlUtils.initialize(logger);
        } catch (PrivilegedActionException e) {
            throw new RuntimeException("Cannot initialise SAML utilities", e);
        }
        wantSignedAuthnRequests = false;
        signingCertificates = new ArrayList<>();
    }

    public SamlIdpMetadataBuilder entityId(String entityId) {
        this.entityId = entityId;
        return this;
    }

    public SamlIdpMetadataBuilder sign(Path certPath) throws CertificateException, IOException {
        var certificates = PemUtils.readCertificates(List.of(certPath))
            .stream()
            .filter(X509Certificate.class::isInstance)
            .map(X509Certificate.class::cast)
            .toList();
        if (certificates.isEmpty()) {
            throw new IllegalArgumentException("No X.509 certificates found in " + certPath.toString());
        }
        return sign(certificates);
    }

    public SamlIdpMetadataBuilder sign(List<X509Certificate> certificates) {
        this.signingCertificates.addAll(certificates);
        return this;
    }

    public String asString() throws CertificateEncodingException {
        var oldLocale = Locale.getDefault();
        try {
            // KeyDescriptorMarshaller calls toString on the SIGNING enum and that may not generate the correct value in other locales
            Locale.setDefault(Locale.ROOT);
            return SamlUtils.getXmlContent(build(), false);
        } finally {
            Locale.setDefault(oldLocale);
        }
    }

    public EntityDescriptor build() throws CertificateEncodingException {
        var roleDescriptor = new IDPSSODescriptorBuilder().buildObject();
        roleDescriptor.removeAllSupportedProtocols();
        roleDescriptor.addSupportedProtocol(SAMLConstants.SAML20P_NS);
        roleDescriptor.setWantAuthnRequestsSigned(wantSignedAuthnRequests);

        for (X509Certificate k : this.signingCertificates) {
            final KeyDescriptor keyDescriptor = new KeyDescriptorBuilder().buildObject();
            keyDescriptor.setUse(UsageType.SIGNING);
            final KeyInfo keyInfo = new KeyInfoBuilder().buildObject();
            KeyInfoSupport.addCertificate(keyInfo, k);
            keyDescriptor.setKeyInfo(keyInfo);
            roleDescriptor.getKeyDescriptors().add(keyDescriptor);
        }

        final EntityDescriptor descriptor = new EntityDescriptorBuilder().buildObject();
        descriptor.setEntityID(this.entityId);
        descriptor.getRoleDescriptors().add(roleDescriptor);

        return descriptor;
    }
}
