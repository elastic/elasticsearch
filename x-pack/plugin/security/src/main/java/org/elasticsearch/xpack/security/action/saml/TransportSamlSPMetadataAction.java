/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action.saml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.saml.SamlSPMetadataAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlSPMetadataRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlSPMetadataResponse;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.saml.SamlMetadataCommand;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlUtils;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorMarshaller;
import org.w3c.dom.Element;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.List;

import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.findSamlRealms;

/**
 * Transport action responsible for generating a SAML SP Metadata.
 */
public class TransportSamlSPMetadataAction
    extends HandledTransportAction<SamlSPMetadataRequest, SamlSPMetadataResponse>  {

    private final Realms realms;

    @Inject
    public TransportSamlSPMetadataAction(TransportService transportService, ActionFilters actionFilters, Realms realms) {
        super(SamlSPMetadataAction.NAME, transportService, actionFilters, SamlSPMetadataRequest::new
        );
        this.realms = realms;
    }

    @Override
    protected void doExecute(Task task, SamlSPMetadataRequest request,
                             ActionListener<SamlSPMetadataResponse> listener) {
        List<SamlRealm> realms = findSamlRealms(this.realms, request.getRealmName(), null);
        if (realms.isEmpty()) {
            listener.onFailure(SamlUtils.samlException("Cannot find any matching realm for [{}]", request));
        } else if (realms.size() > 1) {
            listener.onFailure(SamlUtils.samlException("Found multiple matching realms [{}] for [{}]", realms, request));
        } else {
            prepareMetadata(realms.get(0), listener);
        }
    }

    private void prepareMetadata(SamlRealm realm, ActionListener<SamlSPMetadataResponse> listener) {
        try {
            final EntityDescriptorMarshaller marshaller = new EntityDescriptorMarshaller();
            final EntityDescriptor descriptor = SamlMetadataCommand.buildEntityDescriptorFromSamlRealm(realm);
            final Element element = marshaller.marshall(descriptor);
            final StringWriter writer = new StringWriter();
            final Transformer serializer = SamlUtils.getHardenedXMLTransformer();
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
            serializer.transform(new DOMSource(element), new StreamResult(writer));
            listener.onResponse(new SamlSPMetadataResponse(writer.toString()));
        } catch (Exception e) {
            logger.debug("Internal exception during SAML SP metadata generation", e);
            listener.onFailure(e);
        }
    }
}
