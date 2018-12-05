package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.Action;

public class OpenIdConnectPrepareAuthenticationAction extends Action<OpenIdConnectPrepareAuthenticationResponse> {

    public static final OpenIdConnectPrepareAuthenticationAction INSTANCE = new OpenIdConnectPrepareAuthenticationAction();
    public static final String NAME = "cluster:admin/xpack/security/oidc/prepare";

    protected OpenIdConnectPrepareAuthenticationAction() {
        super(NAME);
    }

    public OpenIdConnectPrepareAuthenticationResponse newResponse() {
        return new OpenIdConnectPrepareAuthenticationResponse();
    }
}
