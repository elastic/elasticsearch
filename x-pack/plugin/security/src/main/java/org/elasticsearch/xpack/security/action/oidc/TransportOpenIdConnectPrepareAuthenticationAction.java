package org.elasticsearch.xpack.security.action.oidc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;

public class TransportOpenIdConnectPrepareAuthenticationAction extends HandledTransportAction<OpenIdConnectPrepareAuthenticateRequest,
    OpenIdConnectPrepareAuthenticationResponse> {

    @Inject
    public TransportOpenIdConnectPrepareAuthenticationAction(ThreadPool threadPool, TransportService transportService,
                                                             ActionFilters actionFilters, AuthenticationService authenticationService,
                                                             TokenService tokenService) {
        super(OpenIdConnectAuthenticateAction.NAME, transportService, actionFilters, OpenIdConnectPrepareAuthenticateRequest::new);
    }

    @Override
    protected void doExecute(Task task, OpenIdConnectPrepareAuthenticateRequest request, ActionListener<OpenIdConnectPrepareAuthenticationResponse> listener) {

    }
}
