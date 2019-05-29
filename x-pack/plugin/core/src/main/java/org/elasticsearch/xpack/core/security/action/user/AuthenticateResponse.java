/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;

public class AuthenticateResponse extends ActionResponse {

    private Authentication authentication;

    public AuthenticateResponse() {}

    public AuthenticateResponse(Authentication authentication){
        this.authentication = authentication;
    }

    public Authentication authentication() {
        return authentication;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_6_6_0)) {
            User.writeTo(authentication.getUser(), out);
        } else {
            authentication.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().before(Version.V_6_6_0)) {
            final User user = User.readFrom(in);
            final Authentication.RealmRef unknownRealm = new Authentication.RealmRef("__unknown", "__unknown", "__unknown");
            authentication = new Authentication(user, unknownRealm, unknownRealm);
        } else {
            authentication = new Authentication(in);
        }
    }

}
