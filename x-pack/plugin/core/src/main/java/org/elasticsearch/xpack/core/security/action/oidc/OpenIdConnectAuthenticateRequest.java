package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

/**
 * Represents a request for authentication using an  OAuth 2.0 Authorization Code
 */
public class OpenIdConnectAuthenticateRequest extends ActionRequest {

    /*
     * OAuth 2.0 Authorization Code
     */
    private String code;

    /*
     * OAuth 2.0 state value.
     */
    private String state;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}

