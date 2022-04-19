/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.security.DelegatePkiAuthenticationRequest;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class SecurityRequestConverters {

    private SecurityRequestConverters() {}

    static Request delegatePkiAuthentication(DelegatePkiAuthenticationRequest delegatePkiAuthenticationRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_security/delegate_pki");
        request.setEntity(createEntity(delegatePkiAuthenticationRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

}
