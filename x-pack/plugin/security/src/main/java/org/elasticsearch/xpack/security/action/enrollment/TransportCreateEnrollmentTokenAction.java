/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.enrollment.CreateEnrollmentTokenAction;
import org.elasticsearch.xpack.core.security.action.enrollment.CreateEnrollmentTokenRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.CreateEnrollmentTokenResponse;
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

import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.StringWriter;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.findSamlRealms;

/**
 * Transport action responsible for creating an enrollment token based on a request.
 */

public class TransportCreateEnrollmentTokenAction
    extends HandledTransportAction<CreateEnrollmentTokenRequest, CreateEnrollmentTokenResponse> {

    @Inject
    public TransportCreateEnrollmentTokenAction(TransportService transportService, ActionFilters actionFilters) {
        super(CreateEnrollmentTokenAction.NAME, transportService, actionFilters, CreateEnrollmentTokenRequest::new
        );
    }

    @Override
    protected void doExecute(Task task, CreateEnrollmentTokenRequest request,
                             ActionListener<CreateEnrollmentTokenResponse> listener) {
        createEnrolmentToken(listener);
    }

    private void createEnrolmentToken(ActionListener<CreateEnrollmentTokenResponse> listener) {
        try {
            String enrollmentTokenString = new String();
            listener.onResponse(new CreateEnrollmentTokenResponse(enrollmentTokenString));
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("Error generating enrollment token", e));
            listener.onFailure(e);
        }
    }
}
