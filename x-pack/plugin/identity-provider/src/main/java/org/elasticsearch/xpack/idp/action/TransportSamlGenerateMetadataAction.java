/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.idp.saml.idp.CloudIdp;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.idp.SamlMetadataGenerator;

public class TransportSamlGenerateMetadataAction extends HandledTransportAction<SamlGenerateMetadataRequest, SamlGenerateMetadataResponse> {

    private final Environment env;

    @Inject
    public TransportSamlGenerateMetadataAction(TransportService transportService, ActionFilters actionFilters, Environment environment) {
        super(SamlGenerateMetadataAction.NAME, transportService, actionFilters, SamlGenerateMetadataRequest::new);
        this.env = environment;
    }

    @Override
    protected void doExecute(Task task, SamlGenerateMetadataRequest request, ActionListener<SamlGenerateMetadataResponse> listener) {
        final SamlIdentityProvider idp = new CloudIdp(env, env.settings());
        final String spEntityId = request.getSpEntityId();
        final SamlMetadataGenerator generator = new SamlMetadataGenerator(idp);
        generator.generateMetadata(spEntityId, listener);
    }
}
