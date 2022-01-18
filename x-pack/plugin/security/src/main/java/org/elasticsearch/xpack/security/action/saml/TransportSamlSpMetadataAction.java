/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.saml;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.saml.SamlSpMetadataAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlSpMetadataRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlSpMetadataResponse;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlSpMetadataBuilder;
import org.elasticsearch.xpack.security.authc.saml.SamlUtils;
import org.elasticsearch.xpack.security.authc.saml.SpConfiguration;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorMarshaller;
import org.w3c.dom.Element;

import java.io.StringWriter;
import java.util.List;
import java.util.Locale;

import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.findSamlRealms;

/**
 * Transport action responsible for generating a SAML SP Metadata.
 */
public class TransportSamlSpMetadataAction extends HandledTransportAction<SamlSpMetadataRequest, SamlSpMetadataResponse> {

    private final Realms realms;

    @Inject
    public TransportSamlSpMetadataAction(TransportService transportService, ActionFilters actionFilters, Realms realms) {
        super(SamlSpMetadataAction.NAME, transportService, actionFilters, SamlSpMetadataRequest::new);
        this.realms = realms;
    }

    @Override
    protected void doExecute(Task task, SamlSpMetadataRequest request, ActionListener<SamlSpMetadataResponse> listener) {
        List<SamlRealm> realms = findSamlRealms(this.realms, request.getRealmName(), null);
        if (realms.isEmpty()) {
            listener.onFailure(SamlUtils.samlException("Cannot find any matching realm for [{}]", request.getRealmName()));
        } else if (realms.size() > 1) {
            listener.onFailure(SamlUtils.samlException("Found multiple matching realms [{}] for [{}]", realms, request.getRealmName()));
        } else {
            prepareMetadata(realms.get(0), listener);
        }
    }

    private void prepareMetadata(SamlRealm realm, ActionListener<SamlSpMetadataResponse> listener) {
        try {
            final EntityDescriptorMarshaller marshaller = new EntityDescriptorMarshaller();
            final SpConfiguration spConfig = realm.getServiceProvider();
            final SamlSpMetadataBuilder builder = new SamlSpMetadataBuilder(Locale.getDefault(), spConfig.getEntityId())
                .assertionConsumerServiceUrl(spConfig.getAscUrl())
                .singleLogoutServiceUrl(spConfig.getLogoutUrl())
                .encryptionCredentials(spConfig.getEncryptionCredentials())
                .signingCredential(spConfig.getSigningConfiguration().getCredential())
                .authnRequestsSigned(spConfig.getSigningConfiguration().shouldSign(AuthnRequest.DEFAULT_ELEMENT_LOCAL_NAME));
            final EntityDescriptor descriptor = builder.build();
            final Element element = marshaller.marshall(descriptor);
            final StringWriter writer = new StringWriter();
            final Transformer serializer = SamlUtils.getHardenedXMLTransformer();
            serializer.transform(new DOMSource(element), new StreamResult(writer));
            listener.onResponse(new SamlSpMetadataResponse(writer.toString()));
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("Error during SAML SP metadata generation for realm [{}]", realm.name()), e);
            listener.onFailure(e);
        }
    }
}
