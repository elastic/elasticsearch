package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a request to prepare an OAuth 2.0 authentication request
 */
public class OpenIdConnectPrepareAuthenticationRequest extends ActionRequest {

    private String realmName;
    private String state;
    private String nonce;

    public String getRealmName() {
        return realmName;
    }

    public String getState() {
        return state;
    }

    public String getNonce() {
        return nonce;
    }

    public void setRealmName(String realmName) {
        this.realmName = realmName;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setNonce(String nonce) {
        this.nonce = nonce;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.hasText(realmName) == false) {
            validationException = addValidationError("realm name must be provided", null);
        }
        if (Strings.hasText(state) == false) {
            validationException = addValidationError("state must be provided", validationException);
        }
        if (Strings.hasText(nonce) == false) {
            validationException = addValidationError("nonce must be provided", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(realmName);
        out.writeString(state);
        out.writeString(nonce);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        realmName = in.readString();
        state = in.readString();
        nonce = in.readString();
    }

    public String toString() {
        return "{realmName=" + realmName + ", state=" + state + ", nonce=" + nonce + "}";
    }

}
