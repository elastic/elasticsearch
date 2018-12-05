package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Represents a request to prepare an OAuth 2.0 authentication request
 */
public class OpenIdConnectPrepareAuthenticationRequest extends ActionRequest {

    @Nullable
    private String realmName;

    public String getRealmName() {
        return realmName;
    }

    public void setRealmName(String realmName) {
        this.realmName = realmName;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(realmName);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        realmName = in.readOptionalString();
    }

    public String toString() {
        return "{realmName=" + realmName + "}";
    }
}
