/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationResponse;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlRedirect;
import org.elasticsearch.xpack.security.authc.saml.SamlUtils;
import org.opensaml.saml.saml2.core.AuthnRequest;

import java.util.List;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.findSamlRealms;

/**
 * Transport action responsible for generating a SAML {@code &lt;AuthnRequest&gt;} as a redirect binding URL.
 */
public final class TransportSamlPrepareAuthenticationAction extends HandledTransportAction<
    SamlPrepareAuthenticationRequest,
    SamlPrepareAuthenticationResponse> {

    private final Realms realms;
    private final Executor genericExecutor;

    @Inject
    public TransportSamlPrepareAuthenticationAction(TransportService transportService, ActionFilters actionFilters, Realms realms) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(
            SamlPrepareAuthenticationAction.NAME,
            transportService,
            actionFilters,
            SamlPrepareAuthenticationRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.realms = realms;
        this.genericExecutor = transportService.getThreadPool().generic();
    }

    @Override
    protected void doExecute(
        Task task,
        SamlPrepareAuthenticationRequest request,
        ActionListener<SamlPrepareAuthenticationResponse> listener
    ) {
        // workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        genericExecutor.execute(ActionRunnable.wrap(listener, l -> doExecuteForked(task, request, l)));
    }

    private void doExecuteForked(
        Task task,
        SamlPrepareAuthenticationRequest request,
        ActionListener<SamlPrepareAuthenticationResponse> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        List<SamlRealm> realms = findSamlRealms(this.realms, request.getRealmName(), request.getAssertionConsumerServiceURL());
        if (realms.isEmpty()) {
            listener.onFailure(SamlUtils.samlException("Cannot find any matching realm for [{}]", request));
        } else if (realms.size() > 1) {
            listener.onFailure(SamlUtils.samlException("Found multiple matching realms [{}] for [{}]", realms, request));
        } else {
            prepareAuthentication(realms.get(0), request.getRelayState(), listener);
        }
    }

    private static void prepareAuthentication(
        SamlRealm realm,
        String relayState,
        ActionListener<SamlPrepareAuthenticationResponse> listener
    ) {
        final AuthnRequest authnRequest = realm.buildAuthenticationRequest();
        try {
            String redirectUrl = new SamlRedirect(authnRequest, realm.getSigningConfiguration()).getRedirectUrl(relayState);
            listener.onResponse(new SamlPrepareAuthenticationResponse(realm.name(), authnRequest.getID(), redirectUrl));
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        }
    }
}
