/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;

public interface CustomServerTransportFilterAuthenticator {
    void authenticate(String securityAction, TransportRequest request, ActionListener<Authentication> authenticationListener);

    class Default implements CustomServerTransportFilterAuthenticator {
        @Override
        public void authenticate(String securityAction, TransportRequest request, ActionListener<Authentication> authenticationListener) {
            // should never be called
            authenticationListener.onFailure(
                new UnsupportedOperationException("CustomServerTransportFilterAuthenticator not implemented for action: " + securityAction)
            );
        }
    }
}
