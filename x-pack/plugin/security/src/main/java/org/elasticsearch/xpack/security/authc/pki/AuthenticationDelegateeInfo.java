/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;
import java.util.Objects;

public class AuthenticationDelegateeInfo implements ToXContentObject {

    final Authentication delegateeClientAuthentication;

    public AuthenticationDelegateeInfo(Authentication delegateeClientAuthentication) {
        this.delegateeClientAuthentication = delegateeClientAuthentication;
    }

    public Authentication getDelegateeClientAuthentication() {
        return delegateeClientAuthentication;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthenticationDelegateeInfo that = (AuthenticationDelegateeInfo) o;
        return delegateeClientAuthentication.equals(that.delegateeClientAuthentication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegateeClientAuthentication);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return delegateeClientAuthentication.toXContent(builder, params);
    }
}
