/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.saml.idp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.idp.action.SamlMetadataResponse;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.opensaml.core.xml.io.MarshallingException;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorMarshaller;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureException;
import org.opensaml.xmlsec.signature.support.Signer;
import org.w3c.dom.Element;

import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;
import static org.opensaml.saml.saml2.core.NameIDType.PERSISTENT;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;
import static org.opensaml.xmlsec.signature.Signature.DEFAULT_ELEMENT_NAME;
import static org.opensaml.xmlsec.signature.support.SignatureConstants.ALGO_ID_C14N_EXCL_OMIT_COMMENTS;
import static org.opensaml.xmlsec.signature.support.SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256;

public class SamlMetadataGenerator {

    private final SamlFactory samlFactory;
    private final SamlIdentityProvider idp;
    private final Logger logger = LogManager.getLogger(SamlMetadataGenerator.class);

    public SamlMetadataGenerator(SamlFactory samlFactory, SamlIdentityProvider idp) {
        this.samlFactory = samlFactory;
        this.idp = idp;
        SamlInit.initialize();
    }

    public void generateMetadata(String spEntityId, String acs, ActionListener<SamlMetadataResponse> listener) {
        idp.resolveServiceProvider(spEntityId, acs, true, ActionListener.wrap(
            sp -> {
                try {
                    if (null == sp) {
                        listener.onFailure(new IllegalArgumentException("Service provider with Entity ID [" + spEntityId
                            + "] is not registered with this Identity Provider"));
                        return;
                    }
                    EntityDescriptor metadata = buildEntityDescriptor(sp);
                    final X509Credential signingCredential = idp.getMetadataSigningCredential();
                    Element metadataElement = possiblySignDescriptor(metadata, signingCredential);
                    listener.onResponse(new SamlMetadataResponse(samlFactory.toString(metadataElement, false)));
                } catch (Exception e) {
                    logger.debug("Error generating IDP metadata to share with [" + spEntityId + "]", e);
                    listener.onFailure(e);
                }
            },
            listener::onFailure
        ));
    }

    EntityDescriptor buildEntityDescriptor(SamlServiceProvider sp) throws Exception {
        final SamlIdPMetadataBuilder builder = new SamlIdPMetadataBuilder(idp.getEntityId())
            .wantAuthnRequestsSigned(sp.shouldSignAuthnRequests())
            .withSingleSignOnServiceUrl(SAML2_REDIRECT_BINDING_URI,
                idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI))
            .withSingleSignOnServiceUrl(SAML2_POST_BINDING_URI,
                idp.getSingleSignOnEndpoint(SAML2_POST_BINDING_URI))
            .withSingleLogoutServiceUrl(SAML2_REDIRECT_BINDING_URI,
                idp.getSingleLogoutEndpoint(SAML2_REDIRECT_BINDING_URI))
            .withSingleLogoutServiceUrl(SAML2_POST_BINDING_URI,
                idp.getSingleLogoutEndpoint(SAML2_POST_BINDING_URI))
            .withNameIdFormat(PERSISTENT)
            .withNameIdFormat(TRANSIENT)
            .organization(idp.getOrganization())
            .withContact(idp.getTechnicalContact());
        final X509Credential signingCredential = idp.getSigningCredential();
        if (null != signingCredential) {
            builder.withSigningCertificate(signingCredential.getEntityCertificate());
        }
        return builder.build();
    }

    Element possiblySignDescriptor(EntityDescriptor descriptor, X509Credential signingCredential) throws MarshallingException,
        SignatureException {
        EntityDescriptorMarshaller marshaller = new EntityDescriptorMarshaller();
        if (null == signingCredential) {
            return marshaller.marshall(descriptor);
        } else {
            Signature signature = samlFactory.buildObject(Signature.class, DEFAULT_ELEMENT_NAME);
            signature.setSigningCredential(signingCredential);
            signature.setSignatureAlgorithm(ALGO_ID_SIGNATURE_RSA_SHA256);
            signature.setCanonicalizationAlgorithm(ALGO_ID_C14N_EXCL_OMIT_COMMENTS);
            descriptor.setSignature(signature);
            Element element = new EntityDescriptorMarshaller().marshall(descriptor);
            Signer.signObject(signature);
            return element;
        }
    }
}
