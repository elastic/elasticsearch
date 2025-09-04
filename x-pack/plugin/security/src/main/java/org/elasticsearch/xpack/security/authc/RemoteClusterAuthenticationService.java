/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.util.Map;

public interface RemoteClusterAuthenticationService {

    void authenticate(String action, TransportRequest request, ActionListener<Authentication> listener);

    void authenticateHeaders(Map<String, String> headers, ActionListener<Void> listener);
}
