package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.Action;

public final class OpenIdConnectAuthenticateAction extends Action<OpenIdConnectAuthenticateResponse> {

    public static final OpenIdConnectAuthenticateAction INSTANCE = new OpenIdConnectAuthenticateAction();
    public static final String NAME = "cluster:admin/xpack/security/oidc/authenticate";

    protected OpenIdConnectAuthenticateAction() {
        super(NAME);
    }

    public OpenIdConnectAuthenticateResponse newResponse() {
        return new OpenIdConnectAuthenticateResponse();
    }
}
