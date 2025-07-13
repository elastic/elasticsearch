/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportRequest;

public interface CustomServerTransportFilter {
    void filter(String securityAction, TransportRequest request, ActionListener<Void> authenticationListener);

    class Default implements CustomServerTransportFilter {
        @Override
        public void filter(String securityAction, TransportRequest request, ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }
}
